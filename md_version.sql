SELECT module_name,version_major,version_minor,version_sub,version_build
      FROM (
                    SELECT module_name,version_major,version_minor,version_sub,version_build,comments,upgrade_at,
                    RANK() OVER (PARTITION BY module_name ORDER BY version_major DESC, version_minor DESC, version_build DESC,version_sub DESC) AS rnk
                    FROM {{schema_name}}.dbo.acm_md_versions
                  ) subquery
WHERE rnk = 1;