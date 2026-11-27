CREATE   PROCEDURE dbo.usp_Bronze_LogStart(
      @RunID        UNIQUEIDENTIFIER
    , @PipelineName VARCHAR(100)
    , @TableName    VARCHAR(100) )
AS
BEGIN
    INSERT INTO dbo.Bronze_Load_Log(RunID, PipelineName, TableName, LoadStartTime, Status)
    VALUES (@RunID, @PipelineName, @TableName, GETDATE(), 'Running');
END

GO