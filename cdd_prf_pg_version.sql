SELECT id, update_date, version_control_label, 'CDD_PRF' AS source
    FROM {{schema_name}}.CDD_PROFILES_VERSIONS
    ORDER BY update_date DESC, id DESC
    LIMIT 1