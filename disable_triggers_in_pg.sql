DO $$
BEGIN
    -- Create the table if it does not exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'target_schema_name'
                   AND table_name = 'trigger_commands') THEN
        CREATE TABLE target_schema_name.trigger_commands (
            disable_command TEXT,
            enable_command TEXT
        );
    END IF;

    -- Insert trigger commands into the table
    INSERT INTO target_schema_name.trigger_commands (disable_command, enable_command)
    SELECT
        'ALTER TABLE ' || event_object_schema || '.' || event_object_table || ' DISABLE TRIGGER ' || trigger_name || ';' AS disable_command,
        'ALTER TABLE ' || event_object_schema || '.' || event_object_table || ' ENABLE TRIGGER ' || trigger_name || ';' AS enable_command
    FROM information_schema.triggers
    WHERE event_object_schema = 'target_schema_name';
END $$;