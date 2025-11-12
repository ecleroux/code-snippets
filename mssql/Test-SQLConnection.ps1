##################################################
#This will connect to a database and select top 1 from a table
#This will happen x times in a row, every y minutes
##################################################

Write-Output "Start - SQL Connection Testing"

$SqlServer = "<ServerName>"
$DatabaseName = "<DBName>"
$UserId = "<UserId>"
$Password = "<Password>"
$Table = "[<SchemaName>].[<TableName>]"

$NumberOfConnectionTestsInARow = 10 # 10 Times in a row
$NumberOfSecondsBetweenTests = 600 # 10 minutes between each test run

for ($i = 2; $i -gt 1; $i++) {
    
    for ($x = 0; $x -lt $NumberOfConnectionTestsInARow; $x++) {

        try {
            $StartT = Get-Date

            $DatabaseConnection = New-Object System.Data.SqlClient.SqlConnection
            $DatabaseConnection.ConnectionString = "Data Source = $SqlServer; Initial Catalog = $DatabaseName; User ID = $UserId; Password = $Password"
            $DatabaseConnection.Open()
            
            $EndT1 = Get-Date

            $SqlCommand = New-Object System.Data.SqlClient.SqlCommand
            $SqlCommand.Connection = $DatabaseConnection
            $SqlCommand.CommandText = "SELECT TOP 1 1 FROM $Table"
            $SqlCommand.CommandTimeout = 0
            $recordCount = $SqlCommand.ExecuteScalar()

            $EndT2 = Get-Date

            $TD1 = NEW-TIMESPAN $StartT $EndT1
            $TD2 = NEW-TIMESPAN $StartT $EndT2

            Write-Output "Ok - $(Get-Date) - $TD1 - $TD2"
        }
        catch {
            Write-Error -Message $_.Exception
            Write-Output "Connection Failed - $(Get-Date)"
            Write-Output -Message $_.Exception
        }
        finally {
            $DatabaseConnection.Dispose()
        }

        [System.Data.SqlClient.SqlConnection]::ClearAllPools()
    }

    Start-Sleep -s $NumberOfSecondsBetweenTests
}