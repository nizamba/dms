--SET STATISTICS TIME ON;
------------------------------------------------------------
-- Gathering Tables information in database
--
-- Num of Rows
-- Is Partitioned
-- Has Lobs
-- Count of Lobs which are NULL or <= 1024 bytes
-- Max Lob Size
--
--
-- Data is saved in table named:  ActMigListTables
--
-------------------------------------------------------------
SET NOCOUNT ON;
IF OBJECT_ID('tempdb..#LOBInfo') IS NOT NULL
    DROP TABLE #LOBInfo;
IF OBJECT_ID('tempdb..#T1') IS NOT NULL
    DROP TABLE #T1;
IF OBJECT_ID('tempdb..#T2') IS NOT NULL
    DROP TABLE #T2;
IF OBJECT_ID('tempdb..#T3') IS NOT NULL
    DROP TABLE #T3;
IF OBJECT_ID('tempdb..#T4') IS NOT NULL
    DROP TABLE #T4;
IF OBJECT_ID('dbo.ActMigListTables') IS NOT NULL
BEGIN
    DROP TABLE dbo.ActMigListTables;
	CREATE TABLE [dbo].[ActMigListTables](
	[ID] INT IDENTITY (1,1) NOT NULL PRIMARY KEY,
	[TableName] [sysname] NOT NULL,
	[SchemaName] [sysname] NOT NULL,
	[rows] [bigint] NULL,
	[IsPartitioned] [varchar](15) NOT NULL,
	[PartitionCount] [int] NULL,
	[ContainsLOBs] [varchar](3) NOT NULL,
	[ConcatenatedLOBs] [nvarchar](max) NOT NULL,
	[CountLess1KAndNulls] [int] NOT NULL,
	[MaxLOBSizeKB] [NUMERIC](18,7) NOT NULL,
	[AvgSizeKB] [int] NOT NULL,
	[MinSizeKB] [int] NOT NULL,
	[MaxSizeMB] [int] NOT NULL,
	[AvgSizeMB] [int] NOT NULL,
	[MinSizeMB] [int] NOT NULL,
	[TableSize] VARCHAR(50) NULL,
	[Type] VARCHAR(255) NULL,
	[Table_JSON] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END
ELSE 
BEGIN
  	CREATE TABLE [dbo].[ActMigListTables](
	[ID] INT IDENTITY (1,1) NOT NULL PRIMARY KEY,
	[TableName] [sysname] NOT NULL,
	[SchemaName] [sysname] NOT NULL,
	[rows] [bigint] NULL,
	[IsPartitioned] [varchar](15) NOT NULL,
	[PartitionCount] [int] NULL,
	[ContainsLOBs] [varchar](3) NOT NULL,
	[ConcatenatedLOBs] [nvarchar](max) NOT NULL,
	[CountLess1KAndNulls] [int] NOT NULL,
	[MaxLOBSizeKB] [NUMERIC](18,7),
	[AvgSizeKB] [int] NOT NULL,
	[MinSizeKB] [int] NOT NULL,
	[MaxSizeMB] [int] NOT NULL,
	[AvgSizeMB] [int] NOT NULL,
	[MinSizeMB] [int] NOT NULL,
	[TableSize] VARCHAR(50) NULL,
	[Type] VARCHAR(255) NULL,
	[Table_JSON] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]	
END;
CREATE TABLE #LOBInfo (
    object_id INT,
    LOBColName NVARCHAR(MAX),
	CountLess1KAndNulls INT,
    [MaxLOBSizeKB] [NUMERIC](18,7),
	[AvgSizeKB] [int],
	[MinSizeKB] [int],
	[MaxSizeMB] [int],
	[AvgSizeMB] [int],
	[MinSizeMB] [int]
);
DECLARE @sql NVARCHAR(MAX) = '';
-- Build dynamic SQL for each LOB column in each table
SELECT @sql += FORMATMESSAGE(
        'INSERT INTO #LOBInfo (object_id, LOBColName, CountLess1KAndNulls, MaxLOBSizeKB, AvgSizeKB, MinSizeKB, MaxSizeMB, AvgSizeMB, MinSizeMB)
         SELECT %d, N''%s'',
            SUM(CASE WHEN %s IS NULL OR DATALENGTH(%s) <= 1024 THEN 1 ELSE 0 END),
            MAX(DATALENGTH(%s)) / 1024.0, -- Max size in KB
            AVG(DATALENGTH(%s)) / 1024.0, -- Avg size in KB
            MIN(DATALENGTH(%s)) / 1024.0, -- Min size in KB
            MAX(DATALENGTH(%s)) / 1048576.0, -- Max size in MB
            AVG(DATALENGTH(%s)) / 1048576.0, -- Avg size in MB
            MIN(DATALENGTH(%s)) / 1048576.0  -- Min size in MB
         FROM %s.%s;'
    , t.object_id, c.name, QUOTENAME(c.name), QUOTENAME(c.name), QUOTENAME(c.name), QUOTENAME(c.name), QUOTENAME(c.name), QUOTENAME(c.name), QUOTENAME(c.name), QUOTENAME(c.name), QUOTENAME(s.name), QUOTENAME(t.name))
FROM sys.columns AS c
JOIN sys.tables AS t ON c.object_id = t.object_id
JOIN sys.schemas AS s ON t.schema_id = s.schema_id
JOIN sys.types AS ty ON c.user_type_id = ty.user_type_id
WHERE ty.name IN ('text', 'ntext', 'image', 'varbinary', 'nvarchar', 'varchar') AND c.max_length = -1;
-- Execute the generated SQL
EXEC sp_executesql @sql;
DELETE T
FROM
(
SELECT *
, DupRank = ROW_NUMBER() OVER (
              PARTITION BY LOBColName,object_id
              ORDER BY (SELECT NULL)
            )
FROM #LOBInfo
) AS T
WHERE DupRank > 1;
-- Define PartitionInfo
WITH PartitionInfo AS (
    SELECT 
        t.object_id,
        SUM(CASE WHEN i.index_id < 2 THEN p.rows ELSE 0 END) AS rows,
        CASE WHEN COUNT(DISTINCT p.partition_number) > 1 THEN COUNT(DISTINCT p.partition_number) ELSE 0 END AS PartitionCount
    FROM 
        sys.tables t
    JOIN sys.indexes i ON t.OBJECT_ID = i.object_id AND i.index_id IN (0, 1)
    JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
    WHERE 
        i.OBJECT_ID > 255 
    GROUP BY 
        t.object_id
),
AggregatedLOBData AS (
    SELECT 
        object_id,
        LOBColName AS ConcatenatedLOBs,
		SUM(CountLess1KAndNulls) AS CountLess1KAndNulls,
        MAX(MaxLOBSizeKB) AS MaxLOBSizeKB,
		[AvgSizeKB],
		[MinSizeKB],
		[MaxSizeMB],
		[AvgSizeMB],
		[MinSizeMB]
    FROM #LOBInfo
    GROUP BY object_id, LOBColName,[AvgSizeKB],[MinSizeKB],[MaxSizeMB],[AvgSizeMB],[MinSizeMB]
)
INSERT INTO ActMigListTables([TableName],[SchemaName],[rows],[IsPartitioned],[PartitionCount],[ContainsLOBs],[ConcatenatedLOBs],[CountLess1KAndNulls],[MaxLOBSizeKB],[AvgSizeKB],[MinSizeKB],[MaxSizeMB],[AvgSizeMB],[MinSizeMB],[Table_JSON],[TableSize])
SELECT DISTINCT 
    t.NAME AS TableName,
    s.Name AS SchemaName,
    pi.rows,
    CASE 
        WHEN pi.PartitionCount > 0 THEN 'Partitioned'
        ELSE 'Not Partitioned'
    END AS IsPartitioned,
    pi.PartitionCount,
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM #LOBInfo li
            WHERE li.object_id = t.object_id
        ) THEN 'Yes'
        ELSE 'No'
    END AS ContainsLOBs,
    ISNULL(ldi.ConcatenatedLOBs, '') AS ConcatenatedLOBs,
	ISNULL(ldi.CountLess1KAndNulls,0) AS CountLess1KAndNulls,
    ISNULL(ldi.MaxLOBSizeKB, 0) AS MaxLOBSizeKB,
	ISNULL(ldi.[AvgSizeKB], 0) AS [AvgSizeKB],
	ISNULL(ldi.[MinSizeKB], 0) AS [MinSizeKB],
	ISNULL(ldi.[MaxSizeMB], 0) AS [MaxSizeMB],
	ISNULL(ldi.[AvgSizeMB], 0) AS [AvgSizeMB],
	ISNULL(ldi.[MinSizeMB], 0) AS [MinSizeMB],
	ISNULL('',NULL) AS Table_JSON,
	CASE 
        WHEN pi.rows < 10000 THEN 'Small'
        WHEN pi.rows BETWEEN 10000 AND 50000 THEN 'Medium'
		when pi.rows BETWEEN 50000 AND 100000 THEN 'Large'
        ELSE 'Extra_Large'
    END AS Type
FROM 
    sys.tables t
INNER JOIN 
    sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN 
    PartitionInfo pi ON t.OBJECT_ID = pi.object_id
LEFT JOIN 
    AggregatedLOBData ldi ON t.OBJECT_ID = ldi.object_id
WHERE 
    t.NAME NOT LIKE 'dt%' 
    AND t.is_ms_shipped = 0
	--AND t.name = 'acm_access_log'
ORDER BY 
    MaxLOBSizeKB DESC;
-- Clean up by dropping the temporary table
DROP TABLE #LOBInfo;
--Select * from ActMigListTables
-------------------------------------------------------------
-- Case 1
-------------------------------------------------------------
-- Table has No Partitions
-- Number of Rows <= 10,000,000
-- Table does not contain lobs or lobs size is <= 1K
--
-- Action Item:
-- Add the table as is, if containts lobs add filter lobsize <= 1K, no parallel load
-------------------------------------------------------------
DECLARE @numLimitRows bigint = 10000000;
DECLARE @numLimitTiles integer = 10;
WITH ActMigTables AS (
select DISTINCT ID,SchemaName,
	   TableName,
	   MAX(PartitionCount) as PartitionCount,
	   MAX(rows) as rows,
	   MAX(ContainsLOBs) as ContainsLOBs,
	   MAX(MaxLOBSizeKB) as MaxLOBSizeKB
	from  ActMigListTables T
	group by SchemaName, TableName,ID
)
select ID,'{"rule-type": "selection","rule-id": "' + cast(100000000+rn as VARCHAR) + '","rule-name": "' + cast(100000000+rn as VARCHAR) + '","object-locator": {"schema-name": "' + SchemaName+ '","table-name": "' + TableName + '"},"rule-action": "include","filters": []}' AS [Table has No Partitions -- Number of Rows <= 10,000,000]
INTO #T1
from (
select ID, SchemaName,TableName,ROW_NUMBER() OVER (ORDER BY SchemaName,TableName) as rn
from ActMigTables
where PartitionCount = 0 and
      rows <= @numLimitRows and
     (ContainsLOBs='No' or (ContainsLOBs='Yes' and MaxLOBSizeKB<=1024)) 
)AS T 

UPDATE ActMigListTables SET ActMigListTables.Table_JSON = #T1.[Table has No Partitions -- Number of Rows <= 10,000,000], Type = '[Table has No Partitions -- Number of Rows <= 10,000,000]'
FROM dbo.ActMigListTables AS ActMigListTables
	INNER JOIN #T1
	ON ActMigListTables.ID = #T1.ID
-------------------------------------------------------------
-- Case 2
-------------------------------------------------------------
-- Table has No Partitions
-- Number of Rows > 10,000,000
-- Table does not contain lobs or lobs size is <= 1K
--
-- Action Item:
-- Add the table with parallel load according to bucketing of data
-- If table contains lobs, add filter lobsize <= 1K
-------------------------------------------------------------
SET @numLimitRows = 10000000;
SET @numLimitTiles = 10;
DECLARE @SchemaName varchar(200);
DECLARE @TableName varchar(200);
DECLARE @PKFirstColumn varchar(200);
DECLARE @NumTiles INTEGER;
DECLARE @ranges VARCHAR(max);
DECLARE @rn INTEGER;
DECLARE tabs_cursor CURSOR FOR
	WITH ActMigTables AS (
	select SchemaName,
		   TableName,
		   MAX(PartitionCount) as PartitionCount,
		   MAX(rows) as rows,
		   MAX(ContainsLOBs) as ContainsLOBs,
		   MAX(MaxLOBSizeKB) as MaxLOBSizeKB
		from  ActMigListTables T
		group by SchemaName, TableName
	)
	select schema_name(tab.schema_id) as [schema_name], 
		tab.[name] as table_name, 
		-- pk.[name] as pk_name,
		-- substring(column_names, 1, len(column_names)-1) as [columns]
		(select col.[name]
						from sys.index_columns ic
							inner join sys.columns col
								on ic.object_id = col.object_id
								and ic.column_id = col.column_id
						where ic.object_id = tab.object_id
							and ic.index_id = pk.index_id
							and ic.index_column_id=1) first_column,
		ROW_NUMBER() OVER (ORDER BY schema_name(tab.schema_id),tab.[name]) as rn
	from sys.tables tab
		left outer join sys.indexes pk
			on tab.object_id = pk.object_id 
			and pk.is_primary_key = 1
	where exists (
		select 1
		from  ActMigTables T
		where T.SchemaName = schema_name(tab.schema_id) and
			  T.TableName = tab.name and
			  T.PartitionCount = 0 and
			  T.rows > @numLimitRows and
		 (T.ContainsLOBs='No' or (T.ContainsLOBs='Yes' and T.MaxLOBSizeKB<=1024))
	);
OPEN tabs_cursor;
FETCH NEXT FROM tabs_cursor INTO @SchemaName, @TableName, @PKFirstColumn,@rn;
WHILE @@FETCH_STATUS = 0
BEGIN
	select TOP 1 @NumTiles=
	      (
		      case
				when ((rows/@numLimitRows)+1 > @numLimitTiles) then @numLimitTiles
				else (rows/@numLimitRows)+1
			  end
	      )
    from ActMigListTables T
	where T.SchemaName = @SchemaName and
	      T.TableName = @TableName;
	DECLARE @sql_version NVARCHAR(128);
	DECLARE @sql_version_num INT;
	-- Get SQL Server version
	SET @sql_version = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR);
	-- Get the major version number
	SET @sql_version_num = CAST(SERVERPROPERTY('ProductMajorVersion') AS INT);
    -- print 'Table: ' + @SchemaName + '.' + @TableName + ', Num of Tiles: ' + cast(@NumTiles as varchar);
	IF @sql_version_num >= 14 -- SQL Server 2017 or later
	BEGIN
		SET @sql = N'
			SELECT @ranges = STRING_AGG(''['' + CAST(min_val AS VARCHAR) + '']'', '''')
			FROM (
				SELECT loc, COUNT(1) AS cnt_vals, MIN(' + @PKFirstColumn + ') AS min_val, MAX(' + @PKFirstColumn + ') AS max_val
				FROM (
					SELECT ' + @PKFirstColumn + ', NTILE(' + CAST(@NumTiles AS VARCHAR) + ') OVER (ORDER BY ' + @PKFirstColumn + ') AS loc
					FROM ' + @SchemaName + '.' + @TableName + '
				) AS t1
				GROUP BY loc
			) AS t2
			WHERE loc > 1;';
	END
	ELSE -- SQL Server 2016
	BEGIN
		SET @sql = N'
			SELECT @ranges = STUFF((SELECT '','' + ''['' + CAST(min_val AS VARCHAR) + '']''
									FROM (
										SELECT loc, COUNT(1) AS cnt_vals, MIN(' + @PKFirstColumn + ') AS min_val, MAX(' + @PKFirstColumn + ') AS max_val
										FROM (
											SELECT ' + @PKFirstColumn + ', NTILE(' + CAST(@NumTiles AS VARCHAR) + ') OVER (ORDER BY ' + @PKFirstColumn + ') AS loc
											FROM ' + @SchemaName + '.' + @TableName + '
										) AS t1
										GROUP BY loc
									) AS t2
									WHERE loc > 1
									FOR XML PATH('''')), 1, 1, '''')';
	END;
	-- Execute the SQL
	EXECUTE sp_executesql @sql, N'@ranges VARCHAR(MAX) OUTPUT', @ranges = @ranges OUTPUT;
	-- print @ranges;
	print '{"rule-type": "selection","rule-id": "' + cast(200000000+@rn as VARCHAR) + '","rule-name": "' + cast(200000000+@rn as VARCHAR) + '","object-locator": {"schema-name": "' + @SchemaName + '","table-name": "' + @TableName + '"},"rule-action": "include"},{"rule-type": "table-settings","rule-id": "' + cast(210000000 + @rn as VARCHAR) + '","rule-name": "' + cast(210000000 + @rn as VARCHAR) + '","object-locator": {"schema-name": "' + @SchemaName + '","table-name": "' + @TableName + '"},"parallel-load": {"type": "ranges","columns": ["' + @PKFirstColumn + '"],"boundaries": [' + @ranges + ']}}';
    FETCH NEXT FROM tabs_cursor INTO @SchemaName, @TableName, @PKFirstColumn, @rn;
END;
CLOSE tabs_cursor;
DEALLOCATE tabs_cursor;

-------------------------------------------------------------
-- Case 3
-------------------------------------------------------------
-- Table has Partitions
-- Table does not contain lobs or lobs size is <= 1K
--
-- Action Item:
-- Add the table with parallel load of partitions
-- If table has lobs, add filter lobsize <= 1K
-------------------------------------------------------------
SET @numLimitRows = 10000000;
SET @numLimitTiles  = 10;
WITH ActMigTables AS (
select ID,SchemaName,
	   TableName,
	   MAX(PartitionCount) as PartitionCount,
	   MAX(rows) as rows,
	   MAX(ContainsLOBs) as ContainsLOBs,
	   MAX(MaxLOBSizeKB) as MaxLOBSizeKB
	from  ActMigListTables T
	group by SchemaName, TableName,ID
)
select ID,'{"rule-type": "selection","rule-id": "' + cast(300000000+rn as VARCHAR) + '","rule-name": "' + cast(300000000+rn as VARCHAR) + '","object-locator": {"schema-name": "' + SchemaName+ '","table-name": "' + TableName + '"},"rule-action": "include"},{"rule-type": "table-settings","rule-id": "' + cast(310000000+rn as VARCHAR) + '","rule-name": "' + cast(310000000+rn as VARCHAR) + '","object-locator": {"schema-name": "' + SchemaName + '","table-name": "' + TableName + '"},"parallel-load": {"type": "partitions-auto"}}' AS [Table has Partitions -- Table does not contain lobs or lobs size is <= 1K]
INTO #T2
from (
select ID,SchemaName,TableName,ROW_NUMBER() OVER (ORDER BY SchemaName,TableName) as rn
from ActMigTables
where PartitionCount > 0 and
     (ContainsLOBs='No' or (ContainsLOBs='Yes' and MaxLOBSizeKB<=1024))
) AS T;

UPDATE ActMigListTables SET ActMigListTables.Table_JSON = #T2.[Table has Partitions -- Table does not contain lobs or lobs size is <= 1K], Type = '[Table has Partitions -- Table does not contain lobs or lobs size is <= 1K]'
FROM dbo.ActMigListTables AS ActMigListTables
	INNER JOIN #T2
	ON ActMigListTables.ID = #T2.ID
-------------------------------------------------------------
-- Case 4
-------------------------------------------------------------
-- Table has No Partitions
-- Table contains lobs > 1K
--
-- Action Item:
-- Add the table with filter of lob size > 1K and adjust Lob Size
-------------------------------------------------------------
SET @numLimitRows = 10000000;
SET @numLimitTiles = 10;
-- DECLARE @SchemaName varchar(200);
-- DECLARE @TableName varchar(200);
-- DECLARE @PKFirstColumn varchar(200);
-- DECLARE @NumTiles INTEGER;
-- DECLARE @sql NVARCHAR(max);
-- DECLARE @ranges VARCHAR(max);
-- DECLARE @rn INTEGER;
-- DECLARE @lobColsFilter VARCHAR(max);
DECLARE tabs_cursor CURSOR FOR
	WITH ActMigTables AS (
	select SchemaName,
		   TableName,
		   MAX(PartitionCount) as PartitionCount,
		   MAX(rows) as rows,
		   MAX(ContainsLOBs) as ContainsLOBs,
		   MAX(MaxLOBSizeKB) as MaxLOBSizeKB
		from  ActMigListTables T
		group by SchemaName, TableName
	)
	select schema_name(tab.schema_id) as [schema_name], 
		tab.[name] as table_name, 
		(select col.[name]
						from sys.index_columns ic
							inner join sys.columns col
								on ic.object_id = col.object_id
								and ic.column_id = col.column_id
						where ic.object_id = tab.object_id
							and ic.index_id = pk.index_id
							and ic.index_column_id=1) first_column,
		ROW_NUMBER() OVER (ORDER BY schema_name(tab.schema_id),tab.[name]) as rn
	from sys.tables tab
		left outer join sys.indexes pk
			on tab.object_id = pk.object_id 
			and pk.is_primary_key = 1
	where exists (
		select 1
		from  ActMigTables T
		where T.SchemaName = schema_name(tab.schema_id) and
			  T.TableName = tab.name and
			  T.PartitionCount = 0 and
		      T.ContainsLOBs='Yes' and T.MaxLOBSizeKB>1024
	);
OPEN tabs_cursor;
FETCH NEXT FROM tabs_cursor INTO @SchemaName, @TableName, @PKFirstColumn,@rn;
WHILE @@FETCH_STATUS = 0
BEGIN
	select TOP 1 @NumTiles=
	      (
		      case
				when ((rows/@numLimitRows)+1 > @numLimitTiles) then @numLimitTiles
				else (rows/@numLimitRows)+1
			  end
	      )
    from ActMigListTables T
	where T.SchemaName = @SchemaName and
	      T.TableName = @TableName;
    -- DECLARE @sql_version NVARCHAR(128);
	-- DECLARE @sql_version_num INT;
	-- Get SQL Server version
	SET @sql_version = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR);
	-- Get the major version number
	SET @sql_version_num = CAST(SERVERPROPERTY('ProductMajorVersion') AS INT);
	IF @sql_version_num >= 14 -- SQL Server 2017 or later
	BEGIN
		SET @sql = N'
			SELECT @ranges = STRING_AGG(''['' + CAST(min_val AS VARCHAR) + '']'', '''')
			FROM (
				SELECT loc, COUNT(1) AS cnt_vals, MIN(' + @PKFirstColumn + ') AS min_val, MAX(' + @PKFirstColumn + ') AS max_val
				FROM (
					SELECT ' + @PKFirstColumn + ', NTILE(' + CAST(@NumTiles AS VARCHAR) + ') OVER (ORDER BY ' + @PKFirstColumn + ') AS loc
					FROM ' + @SchemaName + '.' + @TableName + '
				) AS t1
				GROUP BY loc
			) AS t2
			WHERE loc > 1;';
	END
	ELSE -- SQL Server 2016
	BEGIN
		SET @sql = N'
			SELECT @ranges = STUFF((SELECT '','' + ''['' + CAST(min_val AS VARCHAR) + '']''
									FROM (
										SELECT loc, COUNT(1) AS cnt_vals, MIN(' + @PKFirstColumn + ') AS min_val, MAX(' + @PKFirstColumn + ') AS max_val
										FROM (
											SELECT ' + @PKFirstColumn + ', NTILE(' + CAST(@NumTiles AS VARCHAR) + ') OVER (ORDER BY ' + @PKFirstColumn + ') AS loc
											FROM ' + @SchemaName + '.' + @TableName + '
										) AS t1
										GROUP BY loc
									) AS t2
									WHERE loc > 1
									FOR XML PATH('''')), 1, 1, '''')';
	END;
	-- Execute the SQL
	EXECUTE sp_executesql @sql, N'@ranges VARCHAR(MAX) OUTPUT', @ranges = @ranges OUTPUT;
	-- print @ranges;
	print '{"rule-type": "selection","rule-id": "' + cast(400000000+@rn as VARCHAR) + '","rule-name": "' + cast(400000000+@rn as VARCHAR) + '","object-locator": {"schema-name": "' + @SchemaName + '","table-name": "' + @TableName + '"},"rule-action": "include", "filters": []}';
	if @NumTiles > 1 print ',{"rule-type": "table-settings","rule-id": "' + cast(410000000 + @rn as VARCHAR) + '","rule-name": "' + cast(410000000 + @rn as VARCHAR) + '","object-locator": {"schema-name": "' + @SchemaName + '","table-name": "' + @TableName + '"},"parallel-load": {"type": "ranges","columns": ["' + @PKFirstColumn + '"],"boundaries": [' + @ranges + ']}}';
    FETCH NEXT FROM tabs_cursor INTO @SchemaName, @TableName, @PKFirstColumn, @rn;
END;
CLOSE tabs_cursor;
DEALLOCATE tabs_cursor;

-------------------------------------------------------------
-- Case 5
-------------------------------------------------------------
-- Table has Partitions
-- Table contains lobs > 1K
--
-- Action Item:
-- Add the table with Parallel Load of partitions
-- Add filter of lob size > 1K and adjust Lob Size
-------------------------------------------------------------
SET @numLimitRows = 10000000;
SET @numLimitTiles = 10;
WITH ActMigTables AS (
select ID,SchemaName,
	   TableName,
	   MAX(PartitionCount) as PartitionCount,
	   MAX(rows) as rows,
	   MAX(ContainsLOBs) as ContainsLOBs,
	   MAX(MaxLOBSizeKB) as MaxLOBSizeKB
	from  ActMigListTables T
	group by SchemaName, TableName,ID
)
select ID,'{"rule-type": "selection","rule-id": "' + cast(500000000+rn as VARCHAR) + '","rule-name": "' + cast(500000000+rn as VARCHAR) + '","object-locator": {"schema-name": "' + SchemaName+ '","table-name": "' + TableName + '"},"rule-action": "include"},{"rule-type": "table-settings","rule-id": "' + cast(510000000+rn as VARCHAR) + '","rule-name": "' + cast(510000000+rn as VARCHAR) + '","object-locator": {"schema-name": "' + SchemaName + '","table-name": "' + TableName + '"},"parallel-load": {"type": "partitions-auto"}}' [Table has Partitions -- Table contains lobs > 1K]
INTO #T3
from (
select ID,SchemaName,TableName,ROW_NUMBER() OVER (ORDER BY SchemaName,TableName) as rn
from ActMigTables
where PartitionCount > 0 and
     (ContainsLOBs='Yes' and MaxLOBSizeKB>1024)
) AS T;

UPDATE ActMigListTables SET ActMigListTables.Table_JSON = #T3.[Table has Partitions -- Table contains lobs > 1K], Type = '[Table has Partitions -- Table contains lobs > 1K]'
FROM dbo.ActMigListTables AS ActMigListTables
	INNER JOIN #T3
	ON ActMigListTables.ID = #T3.ID
-------------------------------------------------------------
-- Case 6
-------------------------------------------------------------
-- Table has No Partitions
-- Number of Rows <= 10,000,000
-- Table does contain lobs or lobs size is >= 1K
--
-- Action Item:
-- Add the table as is, if containts lobs add filter lobsize <= 1K, no parallel load
-------------------------------------------------------------
SET @numLimitRows = 10000000;
SET @numLimitTiles = 10;
WITH ActMigTables AS (
select DISTINCT ID,SchemaName,
	   TableName,
	   MAX(PartitionCount) as PartitionCount,
	   MAX(rows) as rows,
	   MAX(ContainsLOBs) as ContainsLOBs,
	   MAX(MaxLOBSizeKB) as MaxLOBSizeKB
	from  ActMigListTables T
	group by SchemaName, TableName,ID
)
select ID,'{"rule-type": "selection","rule-id": "' + cast(100000000+rn as VARCHAR) + '","rule-name": "' + cast(100000000+rn as VARCHAR) + '","object-locator": {"schema-name": "' + SchemaName+ '","table-name": "' + TableName + '"},"rule-action": "include","filters": []}' AS [Table has No Partitions, has LOBs => 1024KB and Number of Rows <= 10,000,000]
INTO #T4
from (
select ID, SchemaName,TableName,ROW_NUMBER() OVER (ORDER BY SchemaName,TableName) as rn
from ActMigTables
where PartitionCount = 0 and
      rows <= @numLimitRows and
     (ContainsLOBs='Yes' AND ContainsLOBs='Yes' and MaxLOBSizeKB>=1024)
)AS T 

UPDATE ActMigListTables SET ActMigListTables.Table_JSON = #T4.[Table has No Partitions, has LOBs => 1024KB and Number of Rows <= 10,000,000], Type = '[Table has No Partitions, has LOBs => 1024KB and Number of Rows <= 10,000,000]'
FROM dbo.ActMigListTables AS ActMigListTables
	INNER JOIN #T4
	ON ActMigListTables.ID = #T4.ID;


SELECT * FROM ActMigListTables
where TableName <> 'ActMigListTables' 
ORDER BY rows desc;