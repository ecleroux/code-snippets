USE [YourDBName]
GO

--This will generate a script that will drop all foreign keys and recreate the foreign keys

SET NOCOUNT ON;

DECLARE @ForeignKeyConstraintName NVARCHAR(500)
		,@SchemaName NVARCHAR(128)
		,@TableName NVARCHAR(128)
		,@ColumnName NVARCHAR(128)
		,@ReferencedSchemaName NVARCHAR(128)
		,@ReferencedTableName NVARCHAR(128)
		,@ReferencedColumnName NVARCHAR(128)
		,@DBName NVARCHAR(128) = DB_NAME()

/*************************************
	Drop All FK Constraints
*************************************/
PRINT '--Drop All FK Constraints'
PRINT ''

DECLARE cursorFKDrop CURSOR FAST_FORWARD READ_ONLY FOR 
	SELECT [obj].[name] AS 'ForeignKeyConstraintName'
		  ,[sch1].[name] AS 'SchemaName'
		  ,[tab1].[name] AS 'TableName'
		  ,[col1].[name] AS 'ColumnName'
		  ,[sch2].[name] AS 'ReferencedSchemaName'
		  ,[tab2].[name] AS 'ReferencedTableName'
		  ,[col2].[name] AS 'ReferencedColumnName'
	FROM [sys].[foreign_key_columns] [fkc]
	INNER JOIN [sys].[objects] [obj]
		ON [obj].[object_id] = [fkc].[constraint_object_id]
	INNER JOIN [sys].[tables] [tab1]
		ON [tab1].[object_id] = [fkc].[parent_object_id]
	INNER JOIN [sys].[schemas] [sch1]
		ON [tab1].[schema_id] = [sch1].[schema_id]
	INNER JOIN [sys].[columns] [col1]
		ON	[col1].[column_id] = [fkc].[parent_column_id]
		AND [col1].[object_id] = [tab1].[object_id]
	INNER JOIN [sys].[tables] [tab2]
		ON [tab2].[object_id] = [fkc].[referenced_object_id]
	INNER JOIN [sys].[columns] [col2]
		ON	[col2].[column_id] = [fkc].[referenced_column_id]
		AND [col2].[object_id] = [tab2].[object_id]
	INNER JOIN [sys].[schemas] [sch2]
		ON [tab2].[schema_id] = [sch2].[schema_id]
	
OPEN cursorFKDrop FETCH NEXT FROM cursorFKDrop INTO @ForeignKeyConstraintName
												,@SchemaName
												,@TableName
												,@ColumnName
												,@ReferencedSchemaName
												,@ReferencedTableName
												,@ReferencedColumnName 
	
WHILE @@FETCH_STATUS = 0
BEGIN
	    
	PRINT 'ALTER TABLE [' + @DBName + '].[' + @SchemaName + '].[' + @TableName + '] DROP CONSTRAINT IF EXISTS [' + @ForeignKeyConstraintName +']'
	PRINT 'GO'
	PRINT ''

	FETCH NEXT FROM cursorFKDrop INTO @ForeignKeyConstraintName
									,@SchemaName
									,@TableName
									,@ColumnName
									,@ReferencedSchemaName
									,@ReferencedTableName
									,@ReferencedColumnName 
END
	
CLOSE cursorFKDrop
DEALLOCATE cursorFKDrop

	
/*************************************
	Create All FK Constraints
*************************************/
PRINT '--Create All FK Constraints'
PRINT ''
DECLARE cursorFKCreate CURSOR FAST_FORWARD READ_ONLY FOR 
	SELECT [obj].[name] AS 'ForeignKeyConstraintName'
		  ,[sch1].[name] AS 'SchemaName'
		  ,[tab1].[name] AS 'TableName'
		  ,[col1].[name] AS 'ColumnName'
		  ,[sch2].[name] AS 'ReferencedSchemaName'
		  ,[tab2].[name] AS 'ReferencedTableName'
		  ,[col2].[name] AS 'ReferencedColumnName'
	FROM [sys].[foreign_key_columns] [fkc]
	INNER JOIN [sys].[objects] [obj]
		ON [obj].[object_id] = [fkc].[constraint_object_id]
	INNER JOIN [sys].[tables] [tab1]
		ON [tab1].[object_id] = [fkc].[parent_object_id]
	INNER JOIN [sys].[schemas] [sch1]
		ON [tab1].[schema_id] = [sch1].[schema_id]
	INNER JOIN [sys].[columns] [col1]
		ON	[col1].[column_id] = [fkc].[parent_column_id]
		AND [col1].[object_id] = [tab1].[object_id]
	INNER JOIN [sys].[tables] [tab2]
		ON [tab2].[object_id] = [fkc].[referenced_object_id]
	INNER JOIN [sys].[columns] [col2]
		ON	[col2].[column_id] = [fkc].[referenced_column_id]
		AND [col2].[object_id] = [tab2].[object_id]
	INNER JOIN [sys].[schemas] [sch2]
		ON [tab2].[schema_id] = [sch2].[schema_id]
	
OPEN cursorFKCreate FETCH NEXT FROM cursorFKCreate INTO @ForeignKeyConstraintName
												,@SchemaName
												,@TableName
												,@ColumnName
												,@ReferencedSchemaName
												,@ReferencedTableName
												,@ReferencedColumnName 
	
WHILE @@FETCH_STATUS = 0
BEGIN
	    
	PRINT 'ALTER TABLE [' + @DBName + '].[' + @SchemaName + '].[' + @TableName + ']  WITH CHECK ADD  CONSTRAINT [' + @ForeignKeyConstraintName +'] FOREIGN KEY([' + @ColumnName +'])
REFERENCES [' + @ReferencedSchemaName + '].[' + @ReferencedTableName + '] ([' + @ReferencedColumnName +'])'
	PRINT 'GO'
	PRINT ''
	PRINT 'ALTER TABLE [' + @DBName + '].[' + @SchemaName + '].[' + @TableName + '] CHECK CONSTRAINT [' + @ForeignKeyConstraintName +']'
	PRINT 'GO'
	PRINT ''

	FETCH NEXT FROM cursorFKCreate INTO @ForeignKeyConstraintName
									,@SchemaName
									,@TableName
									,@ColumnName
									,@ReferencedSchemaName
									,@ReferencedTableName
									,@ReferencedColumnName 
END
	
CLOSE cursorFKCreate
DEALLOCATE cursorFKCreate