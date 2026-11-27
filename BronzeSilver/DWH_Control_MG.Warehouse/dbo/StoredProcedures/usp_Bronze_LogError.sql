CREATE   PROCEDURE dbo.usp_Bronze_LogError(
      @RunID        UNIQUEIDENTIFIER
    , @ErrorMessage VARCHAR(4000) )
AS
BEGIN
    UPDATE dbo.Bronze_Load_Log
    SET   LoadEndTime  = GETDATE()
        , Status       = 'Failed'
        , ErrorMessage = LEFT(@ErrorMessage, 4000)
    WHERE RunID = @RunID;
END

GO