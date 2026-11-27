CREATE   PROCEDURE dbo.usp_write_watermark
    @LastModifiedtime VARCHAR(30),
    @TableName        VARCHAR(255)
AS
BEGIN
    UPDATE dbo.Bronze_Watermark
    SET    WatermarkValue = CAST(@LastModifiedtime AS DATETIME2(3))
    WHERE  TableName = @TableName;
END

GO