-- MSSQL comparison result by using the output from PostgreSQL

-- Step 1: Create a temporary table
IF OBJECT_ID('tempdb..#TempTable') IS NOT NULL
    DROP TABLE #TempTable;

CREATE TABLE #TempTable (
    table_schema NVARCHAR(255),
    table_name NVARCHAR(255),
    row_count BIGINT
);

-- Step 2: Insert the new values into the temporary table
'PostgreSQL_compare_data'

-- Step 3: Do a comparison for the following table's row amount in the SQL server
SELECT DISTINCT
    t1.table_schema AS TempTableSchema,
    t1.table_name AS TempTableName,
    t1.row_count AS TempTableRowCount,
    t2.SchemaName AS ActMigListTablesSchema,
    t2.TableName AS ActMigListTablesName,
    t2.rows AS ActMigListTablesRowCount,
    CASE
        WHEN t1.row_count = t2.rows THEN 'Match'
        ELSE 'Mismatch'
    END AS ComparisonResult
FROM
    #TempTable t1
LEFT JOIN
    [dbo].[ActMigListTables] t2
ON
    t1.table_name COLLATE SQL_Latin1_General_CP1_CI_AS = t2.TableName COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE t1.table_name <> 'ActMigListTables' --t2.SchemaName = 'CDS_UDM_USER'
ORDER BY ActMigListTablesRowCount DESC;

-- Step 4: Provide me with the result if there is a match or mismatch
-- The above query will provide the comparison result

-- Drop the temporary table
DROP TABLE #TempTable;