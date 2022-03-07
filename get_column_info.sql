select 
    concat(
        -- название колонки
        pg_attribute.attname
        , ' '
        --  тип данных (integer и тд)
        , pg_catalog.format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
        -- Может ли быть null
        , CASE WHEN pg_attribute.attnotnull = true THEN ' NOT NULL' END
    ) as column_info
FROM 
    pg_catalog.pg_attribute as pg_attribute 
    INNER JOIN pg_class as pg_class
    ON pg_attribute.attrelid = pg_class.oid
WHERE TRUE 
    AND cast(pg_attribute.attrelid::regclass as text) = '<<<TABLE_NAME>>>'
    AND pg_attribute.attname IN (<<<COLUMNS>>>)
    AND pg_attribute.attnum > 0 
    AND pg_attribute.attisdropped = FALSE
    AND pg_class.relkind = 'r'
;
