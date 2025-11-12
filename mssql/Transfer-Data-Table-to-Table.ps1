##################################################
#Following will copy data from source DB to destination DB
##################################################

$DestinationConnectionString = "Data Source = xxxx; Initial Catalog = DummyData; User ID = xxx; Password = xxx; Connection Timeout=0"
$SourceConnectionString = "Data Source = xxx; Initial Catalog = xxx; Integrated Security=True; Connection Timeout=0"

##################################################
#Transfer Data
##################################################

Write-Output "Transfer Data"
$StartT = Get-Date
    
$SourceDatabaseConnection = New-Object System.Data.SqlClient.SqlConnection
$SourceDatabaseConnection.ConnectionString = $SourceConnectionString
$SourceDatabaseConnection.Open();
    
$SelectSqlCommand = New-Object System.Data.SqlClient.SqlCommand
$SelectSqlCommand.Connection = $SourceDatabaseConnection
$SelectSqlCommand.CommandText = "SELECT * FROM [dbo].[YourTable]"
$SelectSqlCommand.CommandTimeout = 0
    
[System.Data.SqlClient.SqlDataReader] $SqlReader = $SelectSqlCommand.ExecuteReader()
    
Try {

    $BulkCopy = New-Object Data.SqlClient.SqlBulkCopy($DestinationConnectionString)

    #If you need to keep Identity. Use the command below
    $BulkCopy = New-Object Data.SqlClient.SqlBulkCopy($DestinationConnectionString, [System.Data.SqlClient.SqlBulkCopyOptions]::KeepIdentity)         
    $BulkCopy.DestinationTableName = "[dbo].[YourTable]"
    $BulkCopy.BulkCopyTimeout = 0
    $BulkCopy.BatchSize = 100000
    $BulkCopy.WriteToServer($SqlReader)
}
Catch {
    Write-Error -Message $_.Exception
}
Finally {
    $SqlReader.Close()
    $bulkCopy.Close()
    $SourceDatabaseConnection.Close()
    $SourceDatabaseConnection.Dispose()
}
$EndT = Get-Date
$TD1 = NEW-TIMESPAN $StartT $EndT
Write-Output $TD1
