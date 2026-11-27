CREATE   PROCEDURE dbo.usp_Silver_LogSuccess(
      @RunID          UNIQUEIDENTIFIER
    , @RowsRead       INT
    , @RowsValid      INT
    , @RowsRejected   INT
    , @RowsSuperseded INT
    , @Status         VARCHAR(20) )
AS
BEGIN
    UPDATE dbo.Silver_Load_Log
    SET   LoadEndTime    = GETDATE()
        , RowsRead       = @RowsRead
        , RowsValid      = @RowsValid
        , RowsRejected   = @RowsRejected
        , RowsSuperseded = @RowsSuperseded
        , Status         = @Status
    WHERE RunID = @RunID;
END

GO