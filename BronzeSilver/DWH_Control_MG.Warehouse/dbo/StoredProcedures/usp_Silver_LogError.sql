CREATE   PROCEDURE dbo.usp_Silver_LogError(
      @RunID        UNIQUEIDENTIFIER
    , @ErrorMessage VARCHAR(4000) )
AS
BEGIN
    UPDATE dbo.Silver_Load_Log
    SET   LoadEndTime  = GETDATE()
        , Status       = 'Failed'
        , ErrorMessage = LEFT(@ErrorMessage, 4000)
    WHERE RunID = @RunID;
END

GO