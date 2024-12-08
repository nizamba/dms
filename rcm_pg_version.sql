SELECT id, update_date, version_control_label, 'AO' AS source
    FROM {{schema_name}}.case_managment_versions
    ORDER BY update_date DESC, id DESC
    LIMIT 1