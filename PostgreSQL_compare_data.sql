-- PostgreSQL create comparable result and apply it in SQL Server script

-- Step 1: Drop the temporary table if it already exists and create a new one
DROP TABLE IF EXISTS temp_table_summary;
CREATE TEMP TABLE temp_table_summary (
    table_schema TEXT,
    table_name TEXT,
    row_count BIGINT
);

-- Step 2: Declare the schema name and loop through tables to populate the temporary table
DO $$
DECLARE
    schema_name TEXT := 'target_schema_name'; -- Schema name to check tables from
    tbl_name TEXT;                     -- Table name variable
    sql TEXT;                          -- SQL query variable
BEGIN
    -- Step 3: Loop through all the parent tables in the specified schema
    FOR tbl_name IN
        SELECT table_name
        FROM information_schema.tables t
        WHERE table_schema = schema_name
        AND table_type = 'BASE TABLE'
        AND NOT EXISTS (
            SELECT 1
            FROM pg_inherits
            WHERE inhrelid = (quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass
        )
    LOOP
        -- Build the SQL statement dynamically to get the row count for each table
        sql := 'INSERT INTO temp_table_summary (table_schema, table_name, row_count) ' ||
               'SELECT $1, $2, COUNT(*) FROM ' || quote_ident(schema_name) || '.' || quote_ident(tbl_name);

        -- Execute the SQL statement with the schema name and table name as parameters
        EXECUTE sql USING schema_name, tbl_name;
    END LOOP;
END $$;

-- Step 4: Generate INSERT INTO statements
SELECT
    'INSERT INTO #TempTable (table_schema, table_name, row_count) VALUES (' ||
    quote_literal(table_schema) || ', ' ||
    quote_literal(table_name) || ', ' ||
    row_count || ');' AS insert_statement
FROM temp_table_summary
WHERE row_count > 0
ORDER BY row_count DESC;