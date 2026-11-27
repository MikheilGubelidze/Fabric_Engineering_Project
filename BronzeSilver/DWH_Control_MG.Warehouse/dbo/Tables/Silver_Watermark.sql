CREATE TABLE [dbo].[Silver_Watermark] (
    [TableName]      VARCHAR (100) NOT NULL,
    [WatermarkValue] DATETIME2 (6) NOT NULL,
    [UpdatedAt]      DATETIME2 (6) NULL
);


GO