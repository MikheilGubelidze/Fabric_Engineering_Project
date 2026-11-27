CREATE   PROCEDURE dbo.usp_Bronze_LogSuccess(
      @RunID       UNIQUEIDENTIFIER
    , @RowsRead    INT
    , @RowsWritten INT )
AS
BEGIN
    UPDATE dbo.Bronze_Load_Log
    SET   LoadEndTime = GETDATE()
        , RowsRead    = @RowsRead
        , RowsWritten = @RowsWritten
        , Status      = 'Success'
    WHERE RunID = @RunID;
END

GO