SELECT 
  table_name
FROM 
  information_schema.tables
WHERE True
  and table_schema = 'public'
