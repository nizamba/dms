SELECT TOP 1 ID, UPDATE_DATE, VERSION_CONTROL_LABEL, 'CDD_PRF' AS source
    FROM {{schema_name}}.dbo.CDD_PROFILES_VERSIONS
    ORDER BY UPDATE_DATE DESC, ID DESC