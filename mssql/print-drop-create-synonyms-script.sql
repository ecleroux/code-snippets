/********************************************************************************
	This will generate a script that will drop all your synonyms 
	and recreate them
********************************************************************************/

USE [YourDBName]
GO

DECLARE @Print VARCHAR(MAX)

PRINT 'USE [' + DB_NAME() + ']'
PRINT 'GO'
PRINT ''

PRINT '/******************************************************************************'
PRINT '	Drop Synonyms'
PRINT '******************************************************************************/'
PRINT ''

DECLARE cursorDropSynonyms CURSOR FAST_FORWARD READ_ONLY FOR 
	SELECT 'DROP SYNONYM IF EXISTS [' + [name] + ']'
	FROM sys.[synonyms]
	ORDER BY [name]

OPEN cursorDropSynonyms FETCH NEXT FROM cursorDropSynonyms INTO @Print

WHILE @@FETCH_STATUS = 0
BEGIN
    
	PRINT @Print

    FETCH NEXT FROM cursorDropSynonyms INTO @Print
END

CLOSE cursorDropSynonyms
DEALLOCATE cursorDropSynonyms

PRINT ''
PRINT ''
PRINT ''
PRINT '/******************************************************************************'
PRINT '	Create Synonyms'
PRINT '******************************************************************************/'
PRINT ''

DECLARE cursorDropSynonyms CURSOR FAST_FORWARD READ_ONLY FOR 
	SELECT 'CREATE SYNONYM [' + [name] + '] FOR ' + [base_object_name]
	FROM sys.[synonyms]
	ORDER BY [name]

OPEN cursorDropSynonyms FETCH NEXT FROM cursorDropSynonyms INTO @Print

WHILE @@FETCH_STATUS = 0
BEGIN
    
	PRINT @Print

    FETCH NEXT FROM cursorDropSynonyms INTO @Print
END

CLOSE cursorDropSynonyms
DEALLOCATE cursorDropSynonyms

PRINT ''
PRINT 'SELECT * FROM [sys].[synonyms]'
PRINT ''