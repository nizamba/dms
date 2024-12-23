SELECT
	'SELECT setval ('''+
	'<CDD_APP_SCHEMA>.' +
	case
	when (t.name = 'CDD_APP_RISK_LEVELS') then 'cdd_app_seq_risk_levels'
	when (t.name = 'ACTIONS_QUEUE') then 'seq_actions_queue'
	when (t.name = 'FINCEN_LIST') then 'seq_fincen_list'
	when (t.name = 'FINCEN_LIST2') then 'seq_fincen_list2'
	when (t.name = 'FINCEN_LIST3') then 'seq_fincen_list3'
	when (t.name = 'FINCEN_LIST_LOBS') then 'seq_fincen_list_lobs'
	when (t.name = 'IN_REQUESTS_314B') then 'seq_in_requests_314b'
	end
	+ ''',' +
	cast(convert(int,i.LAST_VALUE)+1 as VARCHAR(200)) + ');' seq_alingment_command
FROM
    sys.identity_columns i
JOIN
    sys.columns c ON i.object_id = c.object_id AND i.column_id = c.column_id
JOIN
    sys.tables t ON i.object_id = t.object_id
JOIN
    sys.schemas s ON t.schema_id = s.schema_id
WHERE t.name not in ('ActMigListTables') and i.LAST_VALUE is not NULL;