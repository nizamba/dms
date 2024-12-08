SELECT TOP 1 ID, UPDATE_DATE, VERSION_CONTROL_LABEL, 'UDM' AS source
    FROM (
        SELECT *,
               RANK() OVER (PARTITION BY APP_TYPE ORDER BY UPDATE_DATE DESC) AS rnk
        FROM {{schema_name}}.DW_VERSION
        WHERE APP_TYPE = 'Package'
    ) subquery
    WHERE rnk = 1
    ORDER BY
		CAST(PARSENAME(ID, 4) AS INT) DESC, -- First octet
		CAST(PARSENAME(ID, 3) AS INT) DESC, -- Second octet
		CAST(PARSENAME(ID, 2) AS INT) DESC, -- Third octet
		CAST(PARSENAME(ID, 1) AS INT) DESC -- Fourth octe