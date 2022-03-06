select 'CREATE OR REPLACE TABLE ' || a.attrelid::regclass::text || '(' ||
string_agg(a.attname || ' ' || pg_catalog.format_type(a.atttypid,
a.atttypmod)||
    CASE WHEN
        (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
         FROM pg_catalog.pg_attrdef d
         WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) IS NOT 
NULL THEN
        ' DEFAULT '|| (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                      FROM pg_catalog.pg_attrdef d
                      WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef)
    ELSE
        '' END
||
    CASE WHEN a.attnotnull = true THEN
        ' NOT NULL'
    ELSE
        '' END,E'\n,') || ');'
FROM pg_catalog.pg_attribute a join pg_class on a.attrelid=pg_class.oid
WHERE a.attrelid::regclass::varchar =
'<<<TABLE_NAME>>>' AND a.attname IN (<<<COLUMNS>>>)
AND a.attnum > 0 AND NOT a.attisdropped  and pg_class.relkind='r'
group by a.attrelid;
