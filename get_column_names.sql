SELECT 
  column_name
FROM 
  information_schema.columns
WHERE True 
  and table_schema = 'public' 
  and table_name = '<<<TABLE_NAME>>>';
