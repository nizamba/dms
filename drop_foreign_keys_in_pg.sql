-- Create a table to store the foreign key commands
CREATE TABLE target_schema_name.foreign_key_commands (
    table_name regclass,
    constraint_name name,
    constraint_definition text,
    drop_command text,
    add_command text
);
-- Insert values from the foreign key query into the foreign_key_commands table
INSERT INTO target_schema_name.foreign_key_commands (table_name, constraint_name, constraint_definition, drop_command, add_command)
SELECT 
    con.conrelid::regclass as table_name,
    con.conname as constraint_name,
    pg_get_constraintdef(con.oid) as constraint_definition,
    'alter table '||con.conrelid::regclass||' drop constraint '||con.conname||';' as drop_command,
    'alter table '||con.conrelid::regclass||' add constraint '||con.conname||' '||pg_get_constraintdef(con.oid)||';' as add_command
FROM pg_catalog.pg_constraint con
INNER JOIN pg_catalog.pg_class rel
    ON rel.oid = con.conrelid
INNER JOIN pg_catalog.pg_namespace nsp
    ON nsp.oid = connamespace
WHERE nsp.nspname = 'target_schema_name'
AND con.contype = 'f';