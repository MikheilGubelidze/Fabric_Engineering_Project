# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "80114610-a31d-47e6-92c0-53bedff7117d",
# META       "default_lakehouse_name": "lh_Bronze_MG",
# META       "default_lakehouse_workspace_id": "44d38df2-2370-4631-ae87-8c007e6f1987",
# META       "known_lakehouses": [
# META         {
# META           "id": "80114610-a31d-47e6-92c0-53bedff7117d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


spark.sql("""
CREATE TABLE Raw_Bronze_Data(
    RowID               BIGINT,
    TransactionID       STRING,
    LineNumber          STRING,
    TransactionDateTime STRING,
    StoreID             STRING,
    CustomerID          STRING,
    ProductID           STRING,
    Quantity            STRING,
    UnitPrice           STRING,
    Discount            STRING,
    TotalAmount         STRING,
    PaymentMethod       STRING,
    CashierID           STRING,
    LastModifiedTime    TIMESTAMP)   USING DELTA""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
