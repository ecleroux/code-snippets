/*
================================================================================
SCRIPT: Check and Rebuild Clustered Column Store Indexes
PURPOSE: Diagnose fragmentation in column store indexes and generate 
         reorganization commands for maintenance
================================================================================

USAGE EXAMPLES:
  -- View all column store index health
  EXEC sp_ColumnStoreIndexHealth NULL, NULL, 0;
  
  -- View specific table
  EXEC sp_ColumnStoreIndexHealth NULL, 'FactCreditRatingModel', 0;
  
  -- View only fragmented indexes (>20% deleted rows)
  EXEC sp_ColumnStoreIndexHealth NULL, NULL, 1;
  
  -- Generate and execute maintenance
  EXEC sp_ColumnStoreIndexHealth NULL, NULL, 2;
================================================================================
*/

-- ============================================================================
-- CREATE STORED PROCEDURE FOR REUSABILITY
-- ============================================================================

IF OBJECT_ID('sp_ColumnStoreIndexHealth', 'P') IS NOT NULL
    DROP PROCEDURE sp_ColumnStoreIndexHealth;
GO

CREATE PROCEDURE sp_ColumnStoreIndexHealth
    @SchemaName NVARCHAR(128) = NULL,      -- Filter by schema (NULL = all)
    @TableName NVARCHAR(128) = NULL,       -- Filter by table name (NULL = all)
    @Action INT = 0                        -- 0=Report, 1=Report fragmented only, 2=Generate maintenance
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @CRLF NCHAR(2) = CHAR(13) + CHAR(10);

    -- ========================================================================
    -- PART 1: DIAGNOSIS - Report column store index fragmentation
    -- ========================================================================
    
    IF @Action IN (0, 1)
    BEGIN
        SELECT 
            [i].[object_id],
            OBJECT_SCHEMA_NAME([i].[object_id]) AS [SchemaName],
            OBJECT_NAME([i].[object_id]) AS [TableName],
            [i].[name] AS [IndexName],
            [i].[index_id],
            [i].[type_desc],
            [CSRowGroups].[row_group_id],
            [CSRowGroups].[state_desc],
            [CSRowGroups].[total_rows],
            ISNULL([CSRowGroups].[deleted_rows], 0) AS [DeletedRows],
            CASE 
                WHEN [CSRowGroups].[total_rows] = 0 THEN 0
                ELSE CAST(100.0 * (ISNULL([CSRowGroups].[deleted_rows], 0)) / [CSRowGroups].[total_rows] AS DECIMAL(5, 2))
            END AS [FragmentationPercent],
            CASE 
                WHEN [CSRowGroups].[total_rows] = 0 THEN 0
                ELSE CAST(100.0 * ([CSRowGroups].[total_rows] - ISNULL([CSRowGroups].[deleted_rows], 0)) / [CSRowGroups].[total_rows] AS DECIMAL(5, 2))
            END AS [PercentFull]
        FROM [sys].[indexes] AS [i]
        INNER JOIN [sys].[column_store_row_groups] AS [CSRowGroups]
            ON [i].[object_id] = [CSRowGroups].[object_id]
            AND [i].[index_id] = [CSRowGroups].[index_id]
        WHERE 
            ([i].[type_desc] = 'CLUSTERED COLUMNSTORE' OR [i].[type_desc] = 'NONCLUSTERED COLUMNSTORE')
            AND (@SchemaName IS NULL OR OBJECT_SCHEMA_NAME([i].[object_id]) = @SchemaName)
            AND (@TableName IS NULL OR OBJECT_NAME([i].[object_id]) = @TableName)
            AND (
                @Action = 0
                OR (
                    @Action = 1 
                    AND ISNULL([CSRowGroups].[deleted_rows], 0) > 0
                )
            )
        ORDER BY 
            OBJECT_SCHEMA_NAME([i].[object_id]),
            OBJECT_NAME([i].[object_id]),
            [i].[name],
            [CSRowGroups].[row_group_id];
    END

    -- ========================================================================
    -- PART 2: MAINTENANCE - Generate and optionally execute reorganize commands
    -- ========================================================================
    
    IF @Action = 2
    BEGIN
        -- Build and display maintenance commands
        SELECT 
            OBJECT_SCHEMA_NAME([i].[object_id]) AS [SchemaName],
            OBJECT_NAME([i].[object_id]) AS [TableName],
            [i].[name] AS [IndexName],
            'ALTER INDEX [' + [i].[name] + '] ON [' + OBJECT_SCHEMA_NAME([i].[object_id]) 
                + '].[' + OBJECT_NAME([i].[object_id]) + '] REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);' 
            AS [ReorganizeCommand],
            'ALTER INDEX [' + [i].[name] + '] ON [' + OBJECT_SCHEMA_NAME([i].[object_id]) 
                + '].[' + OBJECT_NAME([i].[object_id]) + '] REORGANIZE;' 
            AS [FinalReorganizeCommand]
        FROM [sys].[indexes] AS [i]
        WHERE 
            ([i].[type_desc] = 'CLUSTERED COLUMNSTORE' OR [i].[type_desc] = 'NONCLUSTERED COLUMNSTORE')
            AND (@SchemaName IS NULL OR OBJECT_SCHEMA_NAME([i].[object_id]) = @SchemaName)
            AND (@TableName IS NULL OR OBJECT_NAME([i].[object_id]) = @TableName)
        ORDER BY 
            OBJECT_SCHEMA_NAME([i].[object_id]),
            OBJECT_NAME([i].[object_id]),
            [i].[name];
    END
END;
GO

-- ============================================================================
-- EXECUTION EXAMPLES
-- ============================================================================

-- View all column store indexes and their fragmentation
EXEC sp_ColumnStoreIndexHealth @SchemaName = NULL, @TableName = NULL, @Action = 0;

-- View only fragmented indexes
-- EXEC sp_ColumnStoreIndexHealth @SchemaName = NULL, @TableName = NULL, @Action = 1;

-- View specific table
-- EXEC sp_ColumnStoreIndexHealth @SchemaName = 'DataWarehouse', @TableName = 'FactCreditRatingModel', @Action = 0;

-- Generate maintenance commands
-- EXEC sp_ColumnStoreIndexHealth @SchemaName = NULL, @TableName = NULL, @Action = 2;