DECLARE  @ServerName VARCHAR(500) = @@SERVERNAME,
        @Database NVARCHAR(128) = N'database-name',
        @LoginUser NVARCHAR(128) = N'login-user',
        @DataReader BIT = 1,
		@DataWriter BIT = 1,
		@GrantExec BIT = 1


PRINT('----------------------------------');
PRINT( CONCAT('Creating login for ', @LoginUser));
PRINT(CONCAT('DataReader: ', @DataReader));
PRINT(CONCAT('DataWriter: ', @DataWriter));
PRINT(CONCAT('GrantExec: ', @GrantExec));
PRINT('----------------------------------');

-- Create login if not exists
IF NOT EXISTS (SELECT name FROM sys.server_principals WHERE name = @LoginUser)
BEGIN
    EXEC('CREATE LOGIN [' + @LoginUser + '] FROM WINDOWS');
END

EXEC ('USE [' + @Database + '];
CREATE USER [' + @GroupUser + '] FROM LOGIN [' + @GroupUser + ']

IF ' + @DBOwner + ' = 1
BEGIN
    ALTER ROLE [db_owner] ADD MEMBER [' + @GroupUser + ']
    PRINT(''Added ' + @GroupUser + ' to db_owner'')
END
IF ' + @DataReader + ' = 1
BEGIN
    ALTER ROLE [db_datareader] ADD MEMBER [' + @GroupUser + ']
    PRINT(''Added ' + @GroupUser + ' to db_datareader'')
END
IF ' + @DataWriter + ' = 1
BEGIN
    ALTER ROLE [db_datawriter] ADD MEMBER [' + @GroupUser + ']
    PRINT(''Added ' + @GroupUser + ' to db_datawriter'')
END
IF ' + @GrantExec + ' = 1
BEGIN
    GRANT EXEC TO [' + @GroupUser + ']
END
');
