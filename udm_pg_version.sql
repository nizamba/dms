SELECT id, update_date, version_control_label, 'UDM' AS source
    FROM (
        SELECT *,
               RANK() OVER (PARTITION BY app_type ORDER BY update_date DESC) AS rnk
        FROM {{schema_name}}.DW_VERSION
        WHERE app_type = 'Package'
    ) subquery
    WHERE rnk = 1
    ORDER BY
        CAST(SPLIT_PART(id, '.', 1) AS INT) DESC,  -- First octet
        CAST(SPLIT_PART(id, '.', 2) AS INT) DESC,  -- Second octet
        CAST(SPLIT_PART(id, '.', 3) AS INT) DESC,  -- Third octet
        CAST(SPLIT_PART(id, '.', 4) AS INT) DESC   -- Fourth octet
    LIMIT 1