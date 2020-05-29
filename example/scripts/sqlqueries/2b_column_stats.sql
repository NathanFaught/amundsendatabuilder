SET NOCOUNT ON

DECLARE @column_statistics AS TABLE(
	[cluster] VARCHAR(255)
	,[db] VARCHAR(10) default('mssql')
	,[schema] VARCHAR(255)
	,[table_name] VARCHAR(255)
	,[col_name] VARCHAR(255)
	,[stat_name] VARCHAR(50)
	,[stat_val] VARCHAR(50)
	,[start_epoch] VARCHAR(50) default('')
	,[end_epoch] VARCHAR(50) default('')
)

DECLARE @COLUMN_NAME VARCHAR(255)
	,@DATA_TYPE VARCHAR(255)
	,@TABLE_CATALOG VARCHAR(255)
	,@TABLE_SCHEMA VARCHAR(255)
	,@TABLE_NAME VARCHAR(255)
	,@CHARACTER_MAXIMUM_LENGTH INT
	,@sql_query NVARCHAR(max)
	,@stat_distinct BIGINT
	,@stat_min VARCHAR(50)
	,@stat_max VARCHAR(50)
	,@stat_numnulls BIGINT
	,@stat_average DECIMAL(38,6)
	,@stat_total BIGINT
	,@stat_numblank BIGINT
	,@tablename_output VARCHAR(255)
	,@servername VARCHAR(255)
	,@start_epoch BIGINT
	,@end_epoch BIGINT

SET @servername = LOWER(REPLACE(@@SERVERNAME,'\','_'))

select @start_epoch = datediff(second,'1970-01-01T00:00:00','2012-05-22T00:00:00')
select @end_epoch = datediff(second,'1970-01-01T00:00:00',GETDATE())

DECLARE column_root CURSOR FOR
	SELECT
		COLUMN_NAME = LOWER(REPLACE(REPLACE(COL.COLUMN_NAME,'\','\\'),'"','\"'))
		,DATA_TYPE = LOWER(COL.DATA_TYPE)
		,TABLE_CATALOG = LOWER(REPLACE(REPLACE(COL.TABLE_CATALOG,'\','\\'),'"','\"'))
		,TABLE_SCHEMA = LOWER(REPLACE(REPLACE(COL.TABLE_SCHEMA,'\','\\'),'"','\"')) 
		,TABLE_NAME = LOWER(REPLACE(REPLACE(COL.TABLE_NAME,'\','\\'),'"','\"'))
		,CHARACTER_MAXIMUM_LENGTH
	FROM INFORMATION_SCHEMA.COLUMNS COL
	JOIN INFORMATION_SCHEMA.TABLES TAB
		ON COL.TABLE_CATALOG = TAB.TABLE_CATALOG
		AND COL.TABLE_SCHEMA = TAB.TABLE_SCHEMA
		AND COL.TABLE_NAME = TAB.TABLE_NAME
	WHERE 1=1 
	AND TAB.TABLE_TYPE = 'BASE TABLE'
	ORDER BY COL.TABLE_SCHEMA, COL.TABLE_NAME, COL.ORDINAL_POSITION

OPEN column_root
FETCH NEXT FROM column_root INTO @COLUMN_NAME, @DATA_TYPE, @TABLE_CATALOG, @TABLE_SCHEMA, @TABLE_NAME, @CHARACTER_MAXIMUM_LENGTH

WHILE @@FETCH_STATUS = 0  
BEGIN
	SET @tablename_output = CONCAT(@TABLE_SCHEMA,'.', @TABLE_NAME)
	PRINT (@tablename_output)
	
	--Numeric
	IF @DATA_TYPE IN ('bigint','numeric','smallint','decimal','smallmoney','int','tinyint','money','float','real')
	BEGIN
		PRINT (@DATA_TYPE + ' ' + @COLUMN_NAME)
		SET @sql_query = N'SELECT @stat_distinct = COUNT(DISTINCT ' + QUOTENAME(@COLUMN_NAME) + ') 
		,@stat_min = CONVERT(VARCHAR(50),CONVERT(DECIMAL(38,6),MIN(' + QUOTENAME(@COLUMN_NAME) + ')))
		,@stat_max = CONVERT(VARCHAR(50),CONVERT(DECIMAL(38,6),MAX(' + QUOTENAME(@COLUMN_NAME) + ')))
		,@stat_numnulls = (SELECT COUNT(1) FROM ' + CONCAT(
										QUOTENAME(@TABLE_CATALOG),'.'
										,QUOTENAME(@TABLE_SCHEMA),'.'
										,QUOTENAME(@TABLE_NAME)) 
										+ ' WHERE ' + QUOTENAME(@COLUMN_NAME) + ' IS NULL)
		,@stat_average = AVG(CAST(' + QUOTENAME(@COLUMN_NAME) + 'AS DECIMAL(38,6)))
		,@stat_total = COUNT(1)
		FROM ' + CONCAT(QUOTENAME(@TABLE_CATALOG),'.',QUOTENAME(@TABLE_SCHEMA),'.',QUOTENAME(@TABLE_NAME))
		--PRINT (@sql_query)
		EXEC sp_executesql @sql_query, N'@stat_distinct BIGINT OUTPUT
										,@stat_min VARCHAR(50) OUTPUT
										,@stat_max VARCHAR(50) OUTPUT
										,@stat_numnulls BIGINT OUTPUT
										,@stat_average DECIMAL(38,6) OUTPUT
										,@stat_total BIGINT OUTPUT'
										,@stat_distinct = @stat_distinct OUTPUT
										,@stat_min = @stat_min OUTPUT
										,@stat_max = @stat_max OUTPUT
										,@stat_numnulls = @stat_numnulls OUTPUT
										,@stat_average = @stat_average OUTPUT
										,@stat_total = @stat_total OUTPUT

		INSERT INTO @column_statistics ([cluster], [schema],[table_name],[col_name],[stat_name],[stat_val],[start_epoch],[end_epoch]) 
		values 
			(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'distinct values'+'"', '"""'+CAST(@stat_distinct AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
			,(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'min'+'"', '"""'+CAST(@stat_min AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
			,(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'max'+'"', '"""'+CAST(@stat_max AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
			,(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'num nulls'+'"', '"""'+CAST(@stat_numnulls AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
			,(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'average'+'"', '"""'+CAST(@stat_average AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
			,(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'total'+'"', '"""'+CAST(@stat_total AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)

	END

	--Datetime
	IF @DATA_TYPE IN ('date','datetimeoffset','datetime2','smalldatetime','datetime','time')
	BEGIN
		PRINT (@DATA_TYPE + ' ' + @COLUMN_NAME)
		SET @sql_query = N'SELECT @stat_distinct = COUNT(DISTINCT ' + QUOTENAME(@COLUMN_NAME) + ') 
		,@stat_min = CONVERT(VARCHAR,MIN(' + QUOTENAME(@COLUMN_NAME) + '),120)
		,@stat_max =  CONVERT(VARCHAR,MAX(' + QUOTENAME(@COLUMN_NAME) + '),120)
		,@stat_numnulls = (SELECT COUNT(1) FROM ' + CONCAT(
									QUOTENAME(@TABLE_CATALOG),'.'
									,QUOTENAME(@TABLE_SCHEMA),'.'
									,QUOTENAME(@TABLE_NAME)) 
									+ ' WHERE ' + QUOTENAME(@COLUMN_NAME) + ' IS NULL)
		,@stat_total = COUNT(1)
		FROM ' + CONCAT(QUOTENAME(@TABLE_CATALOG),'.',QUOTENAME(@TABLE_SCHEMA),'.',QUOTENAME(@TABLE_NAME))
		
		EXEC sp_executesql @sql_query, N'@stat_distinct BIGINT OUTPUT
								,@stat_min VARCHAR(50) OUTPUT
								,@stat_max VARCHAR(50) OUTPUT
								,@stat_numnulls BIGINT OUTPUT
								,@stat_total BIGINT OUTPUT'
								,@stat_distinct = @stat_distinct OUTPUT
								,@stat_min = @stat_min OUTPUT
								,@stat_max = @stat_max OUTPUT
								,@stat_numnulls = @stat_numnulls OUTPUT
								,@stat_total = @stat_total OUTPUT

		INSERT INTO @column_statistics ([cluster], [schema],[table_name],[col_name],[stat_name],[stat_val],[start_epoch],[end_epoch]) 
		values 
			(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'distinct values'+'"', '"""'+CAST(@stat_distinct AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
			,(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'min'+'"', '"""'+CAST(@stat_min AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
			,(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'max'+'"', '"""'+CAST(@stat_max AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
			,(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'num nulls'+'"', '"""'+CAST(@stat_numnulls AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
			,(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'total'+'"', '"""'+CAST(@stat_total AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
	END

	--Strings (short)
	IF @DATA_TYPE IN ('char','varchar','nchar','nvarchar') AND @CHARACTER_MAXIMUM_LENGTH BETWEEN 1 AND 50
	BEGIN
		PRINT (@DATA_TYPE + ' ' + @COLUMN_NAME)
		SET @sql_query = N'SELECT @stat_distinct = COUNT(DISTINCT ' + QUOTENAME(@COLUMN_NAME) + ') 
		,@stat_min = CAST(MIN(' + QUOTENAME(@COLUMN_NAME) + ') AS VARCHAR(20))
		,@stat_max = CAST(MAX(' + QUOTENAME(@COLUMN_NAME) + ') AS VARCHAR(20))
		,@stat_numnulls = (SELECT COUNT(1) FROM ' + CONCAT(
									QUOTENAME(@TABLE_CATALOG),'.'
									,QUOTENAME(@TABLE_SCHEMA),'.'
									,QUOTENAME(@TABLE_NAME)) 
									+ ' WHERE ' + QUOTENAME(@COLUMN_NAME) + ' IS NULL)
		,@stat_total = COUNT(1)
		,@stat_numblank = (SELECT COUNT(1) FROM ' + CONCAT(
									QUOTENAME(@TABLE_CATALOG),'.'
									,QUOTENAME(@TABLE_SCHEMA),'.'
									,QUOTENAME(@TABLE_NAME)) 
									+ ' WHERE ' + QUOTENAME(@COLUMN_NAME) + ' = '''')
		FROM ' + CONCAT(QUOTENAME(@TABLE_CATALOG),'.',QUOTENAME(@TABLE_SCHEMA),'.',QUOTENAME(@TABLE_NAME))
		
		EXEC sp_executesql @sql_query, N'@stat_distinct BIGINT OUTPUT
						,@stat_min VARCHAR(50) OUTPUT
						,@stat_max VARCHAR(50) OUTPUT
						,@stat_numnulls BIGINT OUTPUT
						,@stat_total BIGINT OUTPUT
						,@stat_numblank BIGINT OUTPUT'
						,@stat_distinct = @stat_distinct OUTPUT
						,@stat_min = @stat_min OUTPUT
						,@stat_max = @stat_max OUTPUT
						,@stat_numnulls = @stat_numnulls OUTPUT
						,@stat_total = @stat_total OUTPUT
						,@stat_numblank = @stat_numblank OUTPUT

		/*
		 escaping problem characters: "  \ 
		*/
		SET @stat_max = REPLACE(@stat_max,'\','\\')
		SET @stat_max = REPLACE(@stat_max,'"','')
		SET @stat_min = REPLACE(@stat_min,'\','\\')
		SET @stat_min = REPLACE(@stat_min,'"','')

		INSERT INTO @column_statistics ([cluster], [schema],[table_name],[col_name],[stat_name],[stat_val],[start_epoch],[end_epoch]) 
		values 
			(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'distinct values'+'"', '"""'+CAST(@stat_distinct AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
			,(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'min'+'"', '"""'+CAST(@stat_min AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
			,(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'max'+'"', '"""'+CAST(@stat_max AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
			,(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'num nulls'+'"', '"""'+CAST(@stat_numnulls AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
			,(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'total'+'"', '"""'+CAST(@stat_total AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
			,(@servername,@TABLE_CATALOG,@tablename_output,@COLUMN_NAME,'"'+'num blanks'+'"', '"""'+CAST(@stat_numblank AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
	END

	--Strings (long)
	IF @DATA_TYPE IN ('char','varchar','nchar','nvarchar','ntext','text') AND (@CHARACTER_MAXIMUM_LENGTH < 0 OR @CHARACTER_MAXIMUM_LENGTH > 50)
	BEGIN
		PRINT (@DATA_TYPE + ' ' + @COLUMN_NAME)
		SET @sql_query = N'SELECT
			@stat_numnulls = (SELECT COUNT(1) FROM ' + CONCAT(
									QUOTENAME(@TABLE_CATALOG),'.'
									,QUOTENAME(@TABLE_SCHEMA),'.'
									,QUOTENAME(@TABLE_NAME)) 
									+ ' WHERE ' + QUOTENAME(@COLUMN_NAME) + ' IS NULL)
			,@stat_total = COUNT(1)
			,@stat_numblank = (SELECT COUNT(1) FROM ' + CONCAT(
										QUOTENAME(@TABLE_CATALOG),'.'
										,QUOTENAME(@TABLE_SCHEMA),'.'
										,QUOTENAME(@TABLE_NAME)) 
										+ ' WHERE ' + QUOTENAME(@COLUMN_NAME) + ' = '''')
		FROM ' + CONCAT(QUOTENAME(@TABLE_CATALOG),'.',QUOTENAME(@TABLE_SCHEMA),'.',QUOTENAME(@TABLE_NAME))
		
		EXEC sp_executesql @sql_query, N'@stat_numnulls BIGINT OUTPUT
						,@stat_total BIGINT OUTPUT
						,@stat_numblank BIGINT OUTPUT'
						,@stat_numnulls = @stat_numnulls OUTPUT
						,@stat_total = @stat_total OUTPUT
						,@stat_numblank = @stat_numblank OUTPUT

		INSERT INTO @column_statistics ([cluster], [schema],[table_name],[col_name],[stat_name],[stat_val],[start_epoch],[end_epoch]) 
		values 
			(@servername,@TABLE_CATALOG,CONCAT(@TABLE_SCHEMA,'.', @TABLE_NAME),@COLUMN_NAME,'"'+'num nulls'+'"', '"""'+CAST(@stat_numnulls AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
			,(@servername,@TABLE_CATALOG,CONCAT(@TABLE_SCHEMA,'.', @TABLE_NAME),@COLUMN_NAME,'"'+'total'+'"', '"""'+CAST(@stat_total AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
			,(@servername,@TABLE_CATALOG,CONCAT(@TABLE_SCHEMA,'.', @TABLE_NAME),@COLUMN_NAME,'"'+'num blanks'+'"', '"""'+CAST(@stat_numblank AS VARCHAR(50))+'"""',@start_epoch,@end_epoch)
	END


    FETCH NEXT FROM column_root INTO @COLUMN_NAME, @DATA_TYPE, @TABLE_CATALOG, @TABLE_SCHEMA, @TABLE_NAME, @CHARACTER_MAXIMUM_LENGTH
END
CLOSE column_root
DEALLOCATE column_root


SELECT
	cluster, db, [schema], table_name, [col_name], stat_name
	,stat_val
	, start_epoch, end_epoch
from @column_statistics