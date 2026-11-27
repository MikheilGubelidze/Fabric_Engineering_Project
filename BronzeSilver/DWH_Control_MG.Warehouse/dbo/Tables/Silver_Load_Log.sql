CREATE TABLE [dbo].[Silver_Load_Log] (
    [RunID]          UNIQUEIDENTIFIER NULL,
    [PipelineName]   VARCHAR (100)    NULL,
    [TableName]      VARCHAR (100)    NULL,
    [LoadStartTime]  DATETIME2 (6)    NULL,
    [LoadEndTime]    DATETIME2 (6)    NULL,
    [RowsRead]       INT              NULL,
    [RowsValid]      INT              NULL,
    [RowsRejected]   INT              NULL,
    [RowsSuperseded] INT              NULL,
    [Status]         VARCHAR (20)     NULL,
    [ErrorMessage]   VARCHAR (4000)   NULL
);


GO