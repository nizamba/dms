SELECT
	'SELECT setval ('''+
	'<CDD_PRF_SCHEMA>.' +
	case
      when (t.name = 'CDD_PRF_PARTY_AUDIT') then 'seq_cdd_prf_party_audit'
      when (t.name = 'CDD_PRF_QA_PARTY_COMMENT') then 'seq_cdd_prf_qa_party_comment'
      when (t.name = 'USER_AUDIT') then 'seq_user_audit'
      when (t.name = 'ACTUAL_ANSWERS_COMMENTS') then 'seq_actual_answers_comments'
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
WHERE t.name not in ('ActMigListTables')  and i.LAST_VALUE is not NULL;