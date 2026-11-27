# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1f5375c5-01a1-4f90-be94-39b188df1742",
# META       "default_lakehouse_name": "lh_Silver_MG",
# META       "default_lakehouse_workspace_id": "44d38df2-2370-4631-ae87-8c007e6f1987",
# META       "known_lakehouses": [
# META         {
# META           "id": "80114610-a31d-47e6-92c0-53bedff7117d"
# META         },
# META         {
# META           "id": "1f5375c5-01a1-4f90-be94-39b188df1742"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Parameter cell**

# PARAMETERS CELL ********************

run_id  = "manual-run"
wm_from = "1900-01-01T00:00:00"   # Lookup_Old_WM  → dbo.Silver_Watermark
wm_to   = "1900-01-01T00:00:00"    # Lookup_New_WM  → dbo.Bronze_Watermark

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **1.Import-ები და კონსტანტები**

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import datetime
import json
import time
import sempy.fabric as fabric
spark.conf.set("spark.sql.session.timeZone", "UTC")
BRONZE_TABLE     = "lh_Bronze_MG.dbo.raw_bronze_data"
SILVER_TABLE     = "lh_Silver_MG.dbo.Silver_Sales"
REJECTED_TABLE   = "lh_Silver_MG.dbo.Silver_Rejected"
CHANGE_LOG_TABLE = "lh_Silver_MG.dbo.silver_change_log"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **2.ფანჯრის განსაზღვრა პარამეტრებიდან**

# CELL ********************

def parse_ts(s):
    return datetime.datetime.fromisoformat(s.strip().replace(" ", "T"))
last_wm  = parse_ts(wm_from)
upper_wm = parse_ts(wm_to)

print("Silver Old Watermark (wm_from):", last_wm)
print("Bronze Watermark     (wm_to):  ", upper_wm)

if upper_wm <= last_wm:
    notebookutils.notebook.exit(json.dumps({
        "Status": "NoData", "RowsRead": 0, "RowsValid": 0,
        "RowsRejected": 0, "RowsSuperseded": 0 }))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **3.ინკრემენტალური წაკითხვა**

# CELL ********************

df = (spark.table(BRONZE_TABLE)
      .filter((F.col("LastModifiedTime") > F.lit(last_wm))
            & (F.col("LastModifiedTime") <= F.lit(upper_wm)))) 
rows_read = df.count()
print("Current batch rows:", rows_read)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **4.სვეტებისა და ცარიელი მნიშვნელობების ნორმალიზაცია**

# CELL ********************

df = df.toDF(*[c.strip().lower() for c in df.columns])
text_cols   = [c for c, t in df.dtypes if t == "string"]
null_tokens = ["", "null", "n/a", "na", "none", "-"]
df = df.withColumns({
    c: F.when(F.lower(F.trim(F.col(c))).isin(null_tokens), F.lit(None).cast("string"))
        .otherwise(F.trim(F.col(c)))
    for c in text_cols })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **5.Header სტრიქონების გამოყოფა**

# CELL ********************

header_condition = (
    (F.lower(F.col("transactionid")) == "transactionid")
    | (F.lower(F.col("linenumber")) == "linenumber")
    | (F.lower(F.col("quantity")) == "quantity") )
df_header = df.filter(header_condition).withColumn("RejectReason", F.lit("HEADER_ROW"))
df = df.filter(~header_condition | header_condition.isNull())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **6.რიცხვების, ფასდაკლების და თარიღის ტიპიზაცია**

# CELL ********************

# 12.1 რაოდენობა სიტყვებით
quantity_map  = {"one": "1", "two": "2", "three": "3", "four": "4", "five": "5"}
quantity_expr = F.create_map(*[F.lit(x) for kv in quantity_map.items() for x in kv])
df = (df
    .withColumn("quantity",
        F.coalesce(quantity_expr[F.lower(F.col("quantity"))], F.col("quantity")))
    .withColumn("line_no_clean",      F.expr("try_cast(linenumber AS INT)"))
    .withColumn("quantity_clean",     F.expr("try_cast(quantity AS INT)"))
    .withColumn("unit_price_clean",
        F.expr("try_cast(replace(unitprice, ',', '.') AS DECIMAL(18,2))"))
    .withColumn("total_amount_clean",
        F.expr("try_cast(replace(totalamount, ',', '.') AS DECIMAL(18,2))")) )
# 12.2 ფასდაკლება: "10%", "10", "0.10" -> 0.10
discount_raw = F.expr("try_cast(replace(replace(discount, '%', ''), ',', '.') AS DECIMAL(18,4))")
df = df.withColumn(
    "discount_clean",
    F.when(discount_raw > 1, discount_raw / 100).otherwise(discount_raw).cast("DECIMAL(18,4)") )
# 12.3 თარიღი: შერეული ფორმატები
date_formats = ["yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", "dd/MM/yyyy HH:mm:ss",
                "yyyy.MM.dd HH:mm:ss", "yyyy-MM-dd", "dd-MM-yyyy", "dd/MM/yyyy"]
df = df.withColumn(
    "transaction_ts",
    F.coalesce(*[F.try_to_timestamp(F.col("transactiondatetime"), F.lit(fmt))
                 for fmt in date_formats]) )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **7.მაღაზიისა და გადახდის მეთოდის სტანდარტიზაცია**

# CELL ********************

store_digits = F.regexp_extract(F.upper(F.col("storeid")), r"(\d+)", 1)
payment      = F.lower(F.col("paymentmethod"))
df = (df
    .withColumn("store_id_clean",
        F.when(store_digits != "", F.concat(F.lit("ST"), F.lpad(store_digits, 3, "0"))))
    .withColumn("payment_method_clean",
        F.when(payment.isin("cash", "csh", "ნაღდი"), "CASH")
         .when(payment.isin("card", "visa", "mastercard", "debit card", "credit card", "ბარათი"), "CARD")
         .when(payment.isin("mobile", "mobile pay", "apple pay", "google pay"), "MOBILE")
         .when(payment.isin("bank transfer", "transfer", "bt"), "BANK_TRANSFER")) )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **8.Deduplication: თითოეული RowID-ის ბოლო ვერსია**

# CELL ********************

w_rowid   = Window.partitionBy("rowid").orderBy(F.col("lastmodifiedtime").desc())
df_ranked = df.withColumn("rn", F.row_number().over(w_rowid))
df_superseded = (df_ranked.filter("rn > 1").drop("rn")
                 .withColumn("RejectReason", F.lit("SUPERSEDED_VERSION")))
df_latest = df_ranked.filter("rn = 1").drop("rn")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **9.ვალიდაციის წესები და RejectReason**

# CELL ********************

w_line = (Window.partitionBy("transactionid", "line_no_clean")
          .orderBy(F.col("lastmodifiedtime").desc(), F.col("rowid").desc()))
df_check = df_latest.withColumn("dup_rn", F.row_number().over(w_line))
 
is_return       = F.coalesce(F.upper(F.col("transactionid")).endswith("-R"), F.lit(False))
expected_amount = (F.col("quantity_clean") * F.col("unit_price_clean")
                   * (F.lit(1) - F.col("discount_clean")))
tx_date         = F.to_date("transaction_ts")
 
rules = [
    ("REQUIRED_FIELD_MISSING",
        F.col("transactionid").isNull() | F.col("line_no_clean").isNull()
        | F.col("transaction_ts").isNull() | F.col("store_id_clean").isNull()
        | F.col("productid").isNull() | F.col("quantity_clean").isNull()
        | F.col("unit_price_clean").isNull() | F.col("total_amount_clean").isNull()
        | F.col("payment_method_clean").isNull() | F.col("cashierid").isNull()),
    ("INVALID_PRODUCT_ID",    ~F.coalesce(F.col("productid").rlike(r"^P\d{5}$"), F.lit(False))),
    ("INVALID_QUANTITY",      F.col("quantity_clean").isNull() | (F.col("quantity_clean") == 0)
                              | ((F.col("quantity_clean") < 0) & ~is_return)),
    ("INVALID_UNIT_PRICE",    F.col("unit_price_clean").isNull()
                              | (F.col("unit_price_clean") <= 0)),
    ("INVALID_DISCOUNT",      F.col("discount_clean").isNull()
                              | (F.col("discount_clean") < 0) | (F.col("discount_clean") > 1)),
    ("TOTAL_AMOUNT_MISMATCH", F.col("total_amount_clean").isNull()
                              | (F.abs(F.col("total_amount_clean") - expected_amount) > 0.02)),
    ("INVALID_DATE",          F.col("transaction_ts").isNull() | (tx_date > F.current_date())),
    ("DUPLICATE_TRANSACTION_LINE", F.col("dup_rn") > 1),
]
df_validated = (df_check
    .withColumn("RejectReason",
        F.concat_ws(", ", *[F.when(F.coalesce(cond, F.lit(True)), F.lit(code))
                            for code, cond in rules]))
    .withColumn("ValidationStatus",
        F.when(F.col("RejectReason") == "", "VALID").otherwise("REJECT"))
    .drop("dup_rn")
    .cache())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **10.Silver-ის საბოლოო სქემა და Rejected ნაკადი**

# CELL ********************

df_valid  = df_validated.filter("ValidationStatus = 'VALID'")
df_reject = df_validated.filter("ValidationStatus = 'REJECT'") 
df_silver = df_valid.select(
    F.col("rowid").cast("bigint").alias("RowID"),
    F.col("transactionid").alias("TransactionID"),
    F.col("line_no_clean").alias("LineNumber"),
    F.col("transaction_ts").alias("TransactionDateTime"),
    F.to_date("transaction_ts").alias("TransactionDate"),
    F.col("store_id_clean").alias("StoreID"),
    F.col("customerid").alias("CustomerID"),
    F.col("productid").alias("ProductID"),
    F.col("quantity_clean").alias("Quantity"),
    F.col("unit_price_clean").alias("UnitPrice"),
    F.col("discount_clean").alias("Discount"),
    F.col("total_amount_clean").alias("TotalAmount"),
    F.col("payment_method_clean").alias("PaymentMethod"),
    F.col("cashierid").alias("CashierID"),
    is_return.alias("IsReturn"),
    F.col("lastmodifiedtime").alias("LastModifiedTime"),
    F.lit(run_id).alias("RunID"),
    F.current_timestamp().alias("SilverLoadTime") )
raw_cols = ["rowid", "transactionid", "linenumber", "transactiondatetime","storeid", "customerid","productid",    "quantity", "unitprice","discount", "totalamount", "paymentmethod", "cashierid", "lastmodifiedtime"]
def to_reject_schema(d):
    return d.select(*raw_cols, "RejectReason",
                    F.lit(run_id).alias("RunID"), F.current_timestamp().alias("RejectedAt"))
df_rejected_all = (to_reject_schema(df_header) .unionByName(to_reject_schema(df_reject))
                   .unionByName(to_reject_schema(df_superseded)))
rows_valid      = df_valid.count()
rows_rejected   = df_reject.count() + df_header.count()
rows_superseded = df_superseded.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **11.ცვლილებების ჟურნალი: Silver_Change_Log**

# CELL ********************

batch_ids = df_latest.select(F.col("rowid").cast("bigint").alias("RowID")).distinct()
new_dates = df_silver.select("TransactionDate")
if spark.catalog.tableExists(SILVER_TABLE):
    old_dates = (spark.table(SILVER_TABLE) .join(batch_ids, "RowID", "inner") .select("TransactionDate"))
    changed_dates = new_dates.unionByName(old_dates)
else:
    changed_dates = new_dates
change_log = (changed_dates
    .where(F.col("TransactionDate").isNotNull())
    .distinct()
    .withColumn("RunID", F.lit(run_id))
    .withColumn("LoggedAt", F.current_timestamp())) 
change_log.write.format("delta").mode("append").saveAsTable(CHANGE_LOG_TABLE)
dates_logged = change_log.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **12.Idempotent ჩაწერა (MERGE)**

# CELL ********************

# 18.1 Silver_Sales
if spark.catalog.tableExists(SILVER_TABLE):
    silver = DeltaTable.forName(spark, SILVER_TABLE)
    (silver.alias("t")
        .merge(df_silver.alias("s"), "t.RowID = s.RowID")
        .whenMatchedUpdateAll(condition="s.LastModifiedTime >= t.LastModifiedTime").whenNotMatchedInsertAll()
        .execute())
    # წყაროში შეცვლილი ჩანაწერი, რომელიც ადრე ვალიდური იყო, # ახლა კი REJECT-ია, Silver-იდან იშლება
    reject_ids = df_reject.select(F.col("rowid").cast("bigint").alias("RowID")).distinct()
    (silver.alias("t").merge(reject_ids.alias("r"), "t.RowID = r.RowID").whenMatchedDelete() .execute())
else:
    (df_silver.write.format("delta").option("delta.enableChangeDataFeed", "true")
        .saveAsTable(SILVER_TABLE))
# 18.2 Silver_Rejected
if spark.catalog.tableExists(REJECTED_TABLE):
    (DeltaTable.forName(spark, REJECTED_TABLE).alias("t")
        .merge(df_rejected_all.alias("s"),
               "t.rowid <=> s.rowid AND t.lastmodifiedtime <=> s.lastmodifiedtime "
               "AND t.RejectReason = s.RejectReason").whenNotMatchedInsertAll() .execute())
else:
    df_rejected_all.write.format("delta").saveAsTable(REJECTED_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **13.SQL analytics endpoint-ის metadata-ს განახლება**

# CELL ********************

def refresh_sql_endpoint(lakehouse_name, tables, timeout_sec=600):
    client = fabric.FabricRestClient()
    ws_id  = fabric.get_workspace_id()
    lh_id  = fabric.resolve_item_id(lakehouse_name, type="Lakehouse")
    lh     = client.get(f"v1/workspaces/{ws_id}/lakehouses/{lh_id}").json()
    ep_id  = lh["properties"]["sqlEndpointProperties"]["id"]
    body = {"tables": [{"schema": "dbo", "tableNames": tables}]}
    r = client.post(f"v1/workspaces/{ws_id}/sqlEndpoints/{ep_id}/refreshMetadata", json=body)
    if r.status_code == 202:                       # long-running operation
        op_url, waited, status = r.headers["Location"], 0, None
        while waited < timeout_sec:
            delay = int(r.headers.get("Retry-After", 5))
            time.sleep(delay)
            waited += delay
            r = client.get(op_url)
            status = r.json().get("status")
            if status in ("Succeeded", "Failed"):
                break
        if status != "Succeeded":
            raise Exception(f"SQL endpoint refresh did not succeed: {status}")
    elif r.status_code != 200:
        raise Exception(f"SQL endpoint refresh HTTP {r.status_code}: {r.text[:300]}")
refresh_sql_endpoint("lh_Silver_MG", ["Silver_Sales", "silver_change_log"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **14.შედეგის დაბრუნება Pipeline-ში**

# CELL ********************

df_validated.unpersist()
notebookutils.notebook.exit(json.dumps({
    "Status":         "Success",
    "RowsRead":       rows_read,
    "RowsValid":      rows_valid,
    "RowsRejected":   rows_rejected,
    "RowsSuperseded": rows_superseded,
    "DatesLogged":    dates_logged }))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
