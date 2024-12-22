DO $$
DECLARE
    partitioned_table RECORD;
    partition RECORD;
BEGIN
    -- Loop through all partitioned tables in the specified schema
    FOR partitioned_table IN
        SELECT
            parent.relname AS partitioned_table_name
        FROM
            pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_namespace nsp ON parent.relnamespace = nsp.oid
        WHERE
            nsp.nspname = 'target_schema_name' -- Change target_schema_name as actual target_schema_name
        GROUP BY
            parent.relname
    LOOP
        -- Loop through all partitions of the current partitioned table
        FOR partition IN
            SELECT
                child.relname AS partition_name
            FROM
                pg_inherits
                JOIN pg_class child ON pg_inherits.inhrelid = child.oid
                JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                JOIN pg_namespace nsp ON parent.relnamespace = nsp.oid
            WHERE
                parent.relname = partitioned_table.partitioned_table_name
                AND nsp.nspname = 'target_schema_name' -- Change target_schema_name as actual target_schema_name
                AND child.relkind = 'r' -- Only include tables
        LOOP
			EXECUTE format('ALTER TABLE %I.%I OWNER TO target_schema_name', 'target_schema_name', partition.partition_name); --Change target_schema_name as actual target_schema_name
            --RAISE NOTICE '-- ALTER TABLE %.% OWNER TO target_schema_name;', 'target_schema_name', partition.partition_name; --Change target_schema_name as actual target_schema_name
        END LOOP;
    END LOOP;
END $$;