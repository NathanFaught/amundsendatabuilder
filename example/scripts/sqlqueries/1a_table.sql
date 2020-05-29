SELECT 
[database] = 'mssql'
,cluster = LOWER(REPLACE(@@SERVERNAME,'\','_'))
,[schema] = LOWER(REPLACE(REPLACE(TABLE_CATALOG,'\','\\'),'"','\"'))
,[name] = 
		LOWER(REPLACE(REPLACE(TABLE_SCHEMA,'\','\\'),'"','\"')) 
		+ '.' + 
		LOWER(REPLACE(REPLACE(TABLE_NAME,'\','\\'),'"','\"'))
,[description] = ''
,[tags] = ''
,[is_view] = CASE WHEN TABLE_TYPE = 'VIEW' THEN 'true' ELSE 'false' END
,[description_source] = ''
FROM INFORMATION_SCHEMA.TABLES