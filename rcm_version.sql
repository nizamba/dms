SELECT TOP 1 id, update_date, version_control_label, 'AO' AS source
    FROM {{schema_name}}.dbo.case_managment_versions
 ORDER BY update_date desc,id desc