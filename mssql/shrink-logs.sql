USE [YourDatabase]
GO

DECLARE @DataFileName VARCHAR(255);
SET @DataFileName = (SELECT name
FROM sysfiles
WHERE groupid = 1);

DECLARE @TargetSize INT;
DECLARE @SubtractMB INT = 1000
SET @TargetSize = ROUND(8 * (SELECT size
FROM sysfiles
WHERE groupid = 1) / 1024, 0) - @SubtractMB;

EXEC ('DBCC SHRINKFILE (' + @DataFileName + ', ' + @TargetSize + ')');