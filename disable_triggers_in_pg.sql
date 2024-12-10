-- Create a table to store the trigger enable/disable commands
CREATE TABLE target_schema_name.trigger_commands (
    disable_command text,
    enable_command text
);
-- Insert values from the trigger query into the trigger_commands table
INSERT INTO target_schema_name.trigger_commands (disable_command, enable_command)
SELECT 
    'alter table '||event_object_schema||'.'||event_object_table||' disable trigger '||trigger_name||';' as disable_command,
    'alter table '||event_object_schema||'.'||event_object_table||' enable trigger '||trigger_name||';' as enable_command
FROM information_schema.triggers  
WHERE event_object_schema='target_schema_name';
SELECT * FROM target_schema_name.trigger_commands;