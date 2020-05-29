SELECT
    [name] = LOWER(REPLACE(REPLACE(COLUMN_NAME,'\','\\'),'"','\"'))
	,[description] = ''
	,[col_type] = CASE 
		WHEN DATA_TYPE IN ('char','varchar','nchar','nvarchar','varbinary','binary') AND CHARACTER_MAXIMUM_LENGTH > 0
			THEN LOWER(DATA_TYPE) + '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')'
		WHEN DATA_TYPE IN ('char','varchar','nchar','nvarchar','varbinary','binary') AND CHARACTER_MAXIMUM_LENGTH = -1
			THEN LOWER(DATA_TYPE) + '(max)'
		WHEN DATA_TYPE IN ('decimal') 
			THEN LOWER(DATA_TYPE) + '(' + CAST(NUMERIC_PRECISION AS VARCHAR) + ',' + CAST(NUMERIC_SCALE AS VARCHAR) + ')'
		ELSE LOWER(DATA_TYPE)
		END
	,[sort_order] = ORDINAL_POSITION
	,[database] = 'mssql'
	,[cluster] = LOWER(REPLACE(@@SERVERNAME,'\','_'))
	,[schema] = LOWER(REPLACE(REPLACE(TABLE_CATALOG,'\','\\'),'"','\"'))
	,table_name = 
		LOWER(REPLACE(REPLACE(TABLE_SCHEMA,'\','\\'),'"','\"')) 
		+ '.' + 
		LOWER(REPLACE(REPLACE(TABLE_NAME,'\','\\'),'"','\"'))
FROM INFORMATION_SCHEMA.COLUMNS
ORDER BY table_name, ORDINAL_POSITION
