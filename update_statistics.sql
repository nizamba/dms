ANALYZE pg_catalog.pg_inherits;
ANALYZE pg_catalog.pg_class;
ANALYZE pg_catalog.pg_namespace;
ANALYZE pg_catalog.pg_partitioned_table;
ANALYZE pg_catalog.pg_tablespace;
ANALYZE pg_catalog.pg_attribute;

DO $$  
DECLARE  
    table_name text;  
    schema_name text := 'target_schema_name'; -- Replace this with your schema name
BEGIN  
    SET default_statistics_target = 10000; 
    FOR table_name IN (
        SELECT c.relname 
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_inherits i ON c.oid = i.inhrelid
        WHERE n.nspname = 'target_schema_name'
        AND i.inhrelid IS NULL
        AND c.relkind = 'r'
    )  
    LOOP  
        EXECUTE 'ANALYZE ' || quote_ident('target_schema_name') || '.' || quote_ident(table_name);
    END LOOP;  
END $$;