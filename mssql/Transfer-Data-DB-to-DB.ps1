##################################################
#Following will copy data from source DB to destination DB
#Review [Get table list from destination] section to see which tables will transfered. By default it excludes tSQLt schema and table sysdiagrams
#This script assumes the table structures are the same
# $DeleteDestination if set to 1 will delete and reseed the tables before loading the data. Rather use the TruncateDB.sql script as it is better than a delete reseed
##################################################

workflow Start-MigrateDataDBToDB {

    $DestinationSqlServer = "xxx"
    $SourceSqlServer = "xxx"

    $DestinationDatabaseName = "xxx"
    $SourceDatabaseName = "xxx"

    $DestinationUserId = "xxx"
    $DestinationPassword = "xxx"

    $SourceUserId = "xxx"
    $SourcePassword = "xxx"

    [bit] $DeleteDestination = 0
    [int] $ThrottleLimit = 1
    
    #Username and Password
    $DestinationConnectionString = "Data Source = $DestinationSqlServer; Initial Catalog = $DestinationDataBaseName; User ID = $DestinationUserId; Password = $DestinationPassword; Connection Timeout=0"
    $SourceConnectionString = "Data Source = $SourceSqlServer; Initial Catalog = $SourceDataBaseName; User ID = $SourceUserId; Password = $SourcePassword; Connection Timeout=0"
    
    #Integrated Security 
    #$DestinationConnectionString = "Data Source = $DestinationSqlServer; Initial Catalog = $DestinationDataBaseName; Integrated Security=True; Connection Timeout=0"
    #$SourceConnectionString = "Data Source = $SourceSqlServer; Initial Catalog = $SourceDataBaseName; Integrated Security=True; Connection Timeout=0"

    ##################################################
    #Get table list from destination
    ##################################################
    
    Write-Output "Retrieving Table List from Destination"

    #Update query if you want to remove certain tables
    $Tables = InlineScript {
        $DatabaseConnection = New-Object System.Data.SqlClient.SqlConnection
        $DatabaseConnection.ConnectionString = $Using:DestinationConnectionString
        $DatabaseConnection.Open();
                                                                                
        $SqlCommand = New-Object System.Data.SqlClient.SqlCommand
        $SqlCommand.Connection = $DatabaseConnection
        $SqlCommand.CommandText = "SELECT '[' + [TABLE_SCHEMA] + '].[' + [TABLE_NAME] + ']' AS TableName, OBJECTPROPERTY(OBJECT_ID([TABLE_SCHEMA] + '.' + [TABLE_NAME]), 'TableHasIdentity') AS TableHasIdentity
FROM [INFORMATION_SCHEMA].[TABLES]
WHERE [TABLE_TYPE] = 'BASE TABLE'
AND [TABLE_NAME] NOT IN ('sysdiagrams')
AND [TABLE_SCHEMA] NOT IN ('tSQLt')
ORDER BY [TABLE_SCHEMA],[TABLE_NAME]"
        $SqlCommand.CommandTimeout = 0
        
        $SqlDataAdapter = new-object System.Data.SqlClient.SqlDataAdapter 
        $SqlDataAdapter.SelectCommand = $SqlCommand
        $Dataset = new-object System.Data.Dataset
        $recordCount = $SqlDataAdapter.Fill($Dataset)
        $SqlCommand.Dispose();
        $SqlDataAdapter.Dispose();

        $DatabaseConnection.Close();

        $Dataset.Tables[0]
    }

    ##################################################
    #Set all Constraints to NOCHECK on Destination
    ##################################################

    Write-Output "Setting all Constraints to NOCHECK on Destination"

    ForEach -Parallel -throttlelimit $ThrottleLimit ($row in $Tables) {
        $TableName = $row.TableName

        Write-Output "Setting all Constraints to NOCHECK - $($TableName)"

        InlineScript {
            $DatabaseConnection = New-Object System.Data.SqlClient.SqlConnection
            $DatabaseConnection.ConnectionString = $Using:DestinationConnectionString
            $DatabaseConnection.Open();
                                                                                
            $SqlCommand = New-Object System.Data.SqlClient.SqlCommand
            $SqlCommand.Connection = $DatabaseConnection
            $SqlCommand.CommandText = "ALTER TABLE $Using:TableName NOCHECK CONSTRAINT ALL"

            $SqlCommand.CommandTimeout = 0
            $SqlCommand.ExecuteNonQuery() | out-null
            $SqlCommand.Dispose();

            $DatabaseConnection.Close();
        }
    }

    ##################################################
    #Delete all tables
    ##################################################
    If ($DeleteDestination -eq 1) {

        Write-Output "Deleting from all tables on destination and reseeding to 0"

        ForEach -Parallel -throttlelimit $ThrottleLimit ($row in $Tables) {
            $TableName = $row.TableName

            Write-Output "Delete from - $($TableName) and reseed to 0"

            InlineScript {
                $DatabaseConnection = New-Object System.Data.SqlClient.SqlConnection
                $DatabaseConnection.ConnectionString = $Using:DestinationConnectionString
                $DatabaseConnection.Open();
                                                                                
                $SqlCommand = New-Object System.Data.SqlClient.SqlCommand
                $SqlCommand.Connection = $DatabaseConnection
                $SqlCommand.CommandText = "DELETE FROM $Using:TableName
DBCC CHECKIDENT('$Using:TableName', RESEED, 0)"

                $SqlCommand.CommandTimeout = 0
                $SqlCommand.ExecuteNonQuery() | out-null
                $SqlCommand.Dispose();

                $DatabaseConnection.Close();
            }
        }

    }

    ##################################################
    #Transfer Data
    ##################################################

    Write-Output "Transfer Data"

    ForEach -Parallel -throttlelimit $ThrottleLimit ($row in $Tables) {
        $TableName = $row.TableName
        $TableHasIdentity = $row.TableHasIdentity

        Write-Output "Transfering data - $($TableName)"
            
        InlineScript {

            $SourceDatabaseConnection = New-Object System.Data.SqlClient.SqlConnection
            $SourceDatabaseConnection.ConnectionString = $Using:SourceConnectionString
            $SourceDatabaseConnection.Open();
                        
            $SelectSqlCommand = New-Object System.Data.SqlClient.SqlCommand
            $SelectSqlCommand.Connection = $SourceDatabaseConnection
            $SelectSqlCommand.CommandText = "SELECT * FROM $Using:TableName"
            $SelectSqlCommand.CommandTimeout = 0
                        
            [System.Data.SqlClient.SqlDataReader] $SqlReader = $SelectSqlCommand.ExecuteReader()
                        
            Try {

                If ($Using:TableHasIdentity -eq 1) {
                    $BulkCopy = New-Object Data.SqlClient.SqlBulkCopy($Using:DestinationConnectionString, [System.Data.SqlClient.SqlBulkCopyOptions]::KeepIdentity)
                }
                Else {
                    $BulkCopy = New-Object Data.SqlClient.SqlBulkCopy($Using:DestinationConnectionString)
                }
                                
                $BulkCopy.DestinationTableName = $Using:TableName
                $BulkCopy.BulkCopyTimeout = 0
                $BulkCopy.BatchSize = 10000
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
        }

        Write-Output "Transfering data - $($TableName) - Done"

    }

    ##################################################
    #Set all Constraints to WITH CHECK CHECK CONSTRAINT ALL 
    ##################################################
      
    Write-Output "Set all Constraints to WITH CHECK CHECK CONSTRAINT ALL"

    ForEach -Parallel -throttlelimit $ThrottleLimit ($row in $Tables) {
        $TableName = $row.TableName

        Write-Output "Setting all Constraints to WITH CHECK CHECK CONSTRAINT ALL - $($TableName)"

        InlineScript {
            $DatabaseConnection = New-Object System.Data.SqlClient.SqlConnection
            $DatabaseConnection.ConnectionString = $Using:DestinationConnectionString
            $DatabaseConnection.Open();
                                                                                
            $SqlCommand = New-Object System.Data.SqlClient.SqlCommand
            $SqlCommand.Connection = $DatabaseConnection
            $SqlCommand.CommandText = "ALTER TABLE $Using:TableName WITH CHECK CHECK CONSTRAINT ALL"

            $SqlCommand.CommandTimeout = 0
            $SqlCommand.ExecuteNonQuery() | out-null
            $SqlCommand.Dispose();

            $DatabaseConnection.Close();
        }
    }

    Write-Output "Transfer of all tables are complete, please run DBCC CHECKCONSTRAINTS and rebuild all indexes."
}

Start-MigrateDataDBToDB