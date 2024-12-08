SELECT id, update_date, version_control_label, 'CDD_APP' AS source
    FROM {{schema_name}}.CDD_VERSIONS
    ORDER BY update_date DESC, id DESC