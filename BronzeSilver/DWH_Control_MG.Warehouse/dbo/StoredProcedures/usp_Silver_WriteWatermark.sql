CREATE   PROCEDURE dbo.usp_Silver_WriteWatermark(
      @WatermarkValue VARCHAR(30)
    , @TableName      VARCHAR(100) )
AS
BEGIN
    UPDATE dbo.Silver_Watermark
    SET   WatermarkValue = CAST(@WatermarkValue AS DATETIME2(6))
        , UpdatedAt      = GETDATE()
    WHERE TableName = @TableName   AND WatermarkValue < CAST(@WatermarkValue AS DATETIME2(6));
END

GO