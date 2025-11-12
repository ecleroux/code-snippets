--Check Indexes
SELECT DISTINCT OBJECT_SCHEMA_NAME([ind].[object_id]) AS 'SchemaName'
															  ,OBJECT_NAME([ind].[object_id]) AS 'TableName'
															  ,[ind].[name] AS 'IndexName'
														--,[indexstats].[index_type_desc] AS 'IndexType'
														-- ,[indexstats].[avg_fragmentation_in_percent]
														--,[indexstats].[page_count]
														FROM [sys].[dm_db_index_physical_stats](DB_ID(), NULL, NULL, NULL, NULL) [indexstats]
														INNER JOIN [sys].[indexes] [ind]
															ON	[ind].[object_id] = [indexstats].[object_id]
															AND [ind].[index_id] = [indexstats].[index_id]
														WHERE [indexstats].[avg_fragmentation_in_percent] > 10
														AND	  [indexstats].[page_count] > 10
														AND	  [ind].[name] IS NOT NULL

--Rebuild Indexes
DECLARE @SchemaName NVARCHAR(128)
		,@TableName NVARCHAR(128)
		,@IndexName NVARCHAR(128)
		,@SQL NVARCHAR(MAX)

DECLARE IndexsCurr CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT OBJECT_SCHEMA_NAME([ind].[object_id]) AS 'SchemaName'
															  ,OBJECT_NAME([ind].[object_id]) AS 'TableName'
															  ,[ind].[name] AS 'IndexName'
														FROM [sys].[dm_db_index_physical_stats](DB_ID(), NULL, NULL, NULL, NULL) [indexstats]
														INNER JOIN [sys].[indexes] [ind]
															ON	[ind].[object_id] = [indexstats].[object_id]
															AND [ind].[index_id] = [indexstats].[index_id]
														WHERE [indexstats].[avg_fragmentation_in_percent] > 10
														AND	  [indexstats].[page_count] > 10
														AND	  [ind].[name] IS NOT NULL

OPEN IndexsCurr FETCH NEXT FROM IndexsCurr INTO @SchemaName, @TableName, @IndexName

WHILE @@FETCH_STATUS = 0
BEGIN
    
	PRINT 'Rebuild : ' + @SchemaName + '.' + @TableName + ' - ' + @IndexName

	SET @SQL = 'ALTER INDEX [' + @IndexName + '] ON [' + @SchemaName + '].[' + @TableName + '] REBUILD'

	EXEC sp_executesql @SQL

	PRINT 'Done : ' + @SchemaName + '.' + @TableName + ' - ' + @IndexName
	
    FETCH NEXT FROM IndexsCurr INTO @SchemaName, @TableName, @IndexName
END

CLOSE IndexsCurr
DEALLOCATE IndexsCurr


EXEC sp_updatestats;
