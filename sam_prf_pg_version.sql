SELECT id, update_date, version_control_label, 'SAM_PRF' AS source
    FROM {{schema_name}}.AML_PROFILES_VERSIONS
    ORDER BY update_date DESC
    LIMIT 1