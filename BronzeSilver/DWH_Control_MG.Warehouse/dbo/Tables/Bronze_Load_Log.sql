CREATE TABLE [dbo].[Bronze_Load_Log] (
    [RunID]         UNIQUEIDENTIFIER NULL,
    [PipelineName]  VARCHAR (100)    NULL,
    [TableName]     VARCHAR (100)    NULL,
    [LoadStartTime] DATETIME2 (6)    NULL,
    [LoadEndTime]   DATETIME2 (6)    NULL,
    [RowsRead]      INT              NULL,
    [RowsWritten]   INT              NULL,
    [Status]        VARCHAR (20)     NULL,
    [ErrorMessage]  VARCHAR (4000)   NULL
);


GO