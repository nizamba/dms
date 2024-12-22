--SET STATISTICS TIME ON;

SET NOCOUNT ON;
IF OBJECT_ID('tempdb..#GeneratedSQL') IS NOT NULL DROP TABLE #GeneratedSQL;
CREATE TABLE #GeneratedSQL (ID INT IDENTITY(1,1), SqlStatement NVARCHAR(MAX));
DECLARE @TableName NVARCHAR(128);
DECLARE @SchemaName NVARCHAR(128);
DECLARE @PartitionFunction NVARCHAR(128);
DECLARE @BoundaryValues TABLE (ID INT IDENTITY(1,1), BoundaryValue VARCHAR(50));
DECLARE @SourceSchemaName NVARCHAR(128) = 'dbo';
DECLARE @TargetSchemaName NVARCHAR(128) = 'Target_schema_name'; -- Need to be changed as it is in the PostgreSQL schema
IF OBJECT_ID('tempdb..#PartitionDefinitions') IS NOT NULL DROP TABLE #PartitionDefinitions;
CREATE TABLE #PartitionDefinitions (
    TableName NVARCHAR(255) NOT NULL,
    PartitionType NVARCHAR(50) CHECK (PartitionType IN ('LIST', 'RANGE')) NOT NULL
);
INSERT INTO #PartitionDefinitions (TableName,PartitionType)
VALUES	('partition_test','RANGE');

IF CURSOR_STATUS('global', 'partition_cursor') <> -3
BEGIN
    DEALLOCATE partition_cursor;
END

DECLARE partition_cursor CURSOR FOR
SELECT DISTINCT t.name AS TableName, SCHEMA_NAME(t.schema_id) AS SchemaName, pf.name AS PartitionFunction
FROM sys.tables t
JOIN sys.indexes i ON t.object_id = i.object_id
JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
WHERE p.partition_number > 1 AND SCHEMA_NAME(t.schema_id) = @SourceSchemaName
AND t.name COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TableName FROM #PartitionDefinitions);

OPEN partition_cursor;
FETCH NEXT FROM partition_cursor INTO @TableName, @SchemaName, @PartitionFunction;
WHILE @@FETCH_STATUS = 0
BEGIN
    DELETE FROM @BoundaryValues;
    INSERT INTO @BoundaryValues (BoundaryValue)
    SELECT CAST(prv.value AS VARCHAR(100)) AS PartitionBoundaryValue
    FROM sys.dm_db_partition_stats AS pstats
    INNER JOIN sys.partitions AS p ON pstats.partition_id = p.partition_id
    INNER JOIN sys.destination_data_spaces AS dds ON pstats.partition_number = dds.destination_id
    INNER JOIN sys.data_spaces AS ds ON dds.data_space_id = ds.data_space_id
    INNER JOIN sys.partition_schemes AS ps ON dds.partition_scheme_id = ps.data_space_id
    INNER JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
    INNER JOIN sys.indexes AS i ON pstats.object_id = i.object_id AND pstats.index_id = i.index_id AND dds.partition_scheme_id = i.data_space_id AND i.type <= 1 /* Heap or Clustered Index */
    INNER JOIN sys.index_columns AS ic ON i.index_id = ic.index_id AND i.object_id = ic.object_id AND ic.partition_ordinal > 0
    INNER JOIN sys.columns AS c ON pstats.object_id = c.object_id AND ic.column_id = c.column_id
    LEFT JOIN sys.partition_range_values AS prv ON pf.function_id = prv.function_id AND pstats.partition_number = (CASE pf.boundary_value_on_right WHEN 0 THEN prv.boundary_id ELSE (prv.boundary_id+1) END)
    WHERE pf.name = @PartitionFunction and OBJECT_NAME(pstats.object_id) = @TableName;;

    IF NOT EXISTS (SELECT 1 FROM #GeneratedSQL WHERE SqlStatement LIKE '%DROP TABLE IF EXISTS%'+ @TableName + '%')
    BEGIN
        INSERT INTO #GeneratedSQL (SqlStatement)
        VALUES ('DO $$ DECLARE partition_name text; BEGIN FOR partition_name IN SELECT inhrelid::regclass::text FROM pg_inherits WHERE pg_inherits.inhparent = ''' + @TargetSchemaName + '.' + @TableName + '''::regclass LOOP EXECUTE ''DROP TABLE IF EXISTS '' || partition_name || '' CASCADE;''; END LOOP; END $$; ');
    END

    DECLARE @PartitionType NVARCHAR(50);
    SELECT @PartitionType = PartitionType FROM #PartitionDefinitions WHERE TableName = @TableName;
    DECLARE @PartitionIndex INT = 0, @CurrentBoundaryValue VARCHAR(50), @NextBoundaryValue VARCHAR(50);
    IF CURSOR_STATUS('local', 'boundary_cursor') <> -3
    BEGIN
        CLOSE boundary_cursor;
        DEALLOCATE boundary_cursor;
    END

    DECLARE @BoundaryValueIsNullAndIdIsOne BIT;
    -- Check if the BoundaryValue is NULL and the ID is 1
    SELECT @BoundaryValueIsNullAndIdIsOne =
        CASE
            WHEN BoundaryValue IS NULL AND ID = 1 THEN 1
            ELSE 0
        END
    FROM @BoundaryValues
    WHERE ID = 1;

    --PRINT 'Debug: @BoundaryValueIsNullAndIdIsOne = ' + CAST(@BoundaryValueIsNullAndIdIsOne AS NVARCHAR);

    DECLARE boundary_cursor CURSOR FOR
    SELECT BoundaryValue FROM @BoundaryValues ORDER BY ID;
    OPEN boundary_cursor;
    FETCH NEXT FROM boundary_cursor INTO @CurrentBoundaryValue;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        FETCH NEXT FROM boundary_cursor INTO @NextBoundaryValue;
        SET @PartitionIndex = @PartitionIndex + 1;
        DECLARE @SqlStatement NVARCHAR(MAX);

        --PRINT 'Debug: @CurrentBoundaryValue = ' + ISNULL(@CurrentBoundaryValue, 'NULL');
        --PRINT 'Debug: @NextBoundaryValue = ' + ISNULL(@NextBoundaryValue, 'NULL');

        IF @PartitionType = 'RANGE'
        BEGIN
            IF @BoundaryValueIsNullAndIdIsOne = 1 AND @CurrentBoundaryValue IS NULL AND @PartitionIndex = 1
            BEGIN
                --PRINT 'Debug: Entered @BoundaryValueIsNullAndIdIsOne = 1 block';
                SET @SqlStatement = 'CREATE TABLE ' + @TargetSchemaName + '.' + @TableName + '_p' + CAST(@PartitionIndex AS NVARCHAR) +
                ' PARTITION OF ' + @TargetSchemaName + '.' + @TableName +
                ' FOR VALUES FROM (MINVALUE) TO (''' + @NextBoundaryValue + ''');';
            END

            ELSE IF @NextBoundaryValue IS NOT NULL AND @CurrentBoundaryValue <> @NextBoundaryValue
            BEGIN
                --PRINT 'Debug: Creating partition with specific range';
                SET @SqlStatement = 'CREATE TABLE ' + @TargetSchemaName + '.' + @TableName + '_p' + CAST(@PartitionIndex AS NVARCHAR) +
                ' PARTITION OF ' + @TargetSchemaName + '.' + @TableName +
                ' FOR VALUES FROM (''' + @CurrentBoundaryValue + ''') TO (''' + @NextBoundaryValue + ''');';
            END
            ELSE -- Handling the last partition to MAXVALUE
            BEGIN
                --PRINT 'Debug: Creating partition to MAXVALUE';
                SET @SqlStatement = 'CREATE TABLE ' + @TargetSchemaName + '.' + @TableName + '_p' + CAST(@PartitionIndex AS NVARCHAR) +
                ' PARTITION OF ' + @TargetSchemaName + '.' + @TableName +
                ' FOR VALUES FROM (''' + @CurrentBoundaryValue + ''') TO (MAXVALUE);';
            END
        END
        ELSE IF @PartitionType = 'LIST'
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM #GeneratedSQL WHERE SqlStatement LIKE '%' + @TableName + '_p' + CAST(@PartitionIndex AS NVARCHAR) + '% FOR VALUES IN (''' + REPLACE(@CurrentBoundaryValue,'Z','') + ''')')
            BEGIN
                SET @SqlStatement = 'CREATE TABLE ' + @TargetSchemaName + '.' + @TableName + '_p' + CAST(@PartitionIndex AS NVARCHAR) +
                ' PARTITION OF ' + @TargetSchemaName + '.' + @TableName +
                ' FOR VALUES IN (''' + REPLACE(@CurrentBoundaryValue,'Z','') + ''');';
                INSERT INTO #GeneratedSQL (SqlStatement) VALUES (@SqlStatement);
            END
        END

        IF @SqlStatement IS NOT NULL AND NOT EXISTS (SELECT 1 FROM #GeneratedSQL WHERE SqlStatement = @SqlStatement)
        BEGIN
            INSERT INTO #GeneratedSQL (SqlStatement) VALUES (@SqlStatement);
        END

        SET @CurrentBoundaryValue = @NextBoundaryValue;
    END;
    CLOSE boundary_cursor;
    DEALLOCATE boundary_cursor;
    FETCH NEXT FROM partition_cursor INTO @TableName, @SchemaName, @PartitionFunction;
END;
CLOSE partition_cursor;
DEALLOCATE partition_cursor;

SELECT SqlStatement FROM #GeneratedSQL
WHERE SqlStatement IS NOT NULL
ORDER BY ID;

DROP TABLE #GeneratedSQL;