-- Run script on you SQL DB server 
USE master
GO

IF EXISTS (SELECT * FROM sys.databases WHERE name = 'db_monitor')
BEGIN
	DROP DATABASE [db_monitor]
END

CREATE DATABASE [db_monitor];
GO
ALTER LOGIN [sa] WITH PASSWORD=N'pas$w0rd'
GO

USE [db_monitor]
GO
CREATE SCHEMA [Graphite]
GO

CREATE FUNCTION [Graphite].[remove_special_characters]  (@name VARCHAR(max))
RETURNS VARCHAR(MAX)
AS
BEGIN
	RETURN	LOWER(REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(
								REPLACE(
										REPLACE(
											REPLACE(
												REPLACE(
													REPLACE(
														REPLACE(
															REPLACE (
																REPLACE(
																	REPLACE(
																		REPLACE(
																			REPLACE(
																				REPLACE(
																					REPLACE(
																						REPLACE(
																								REPLACE(@name
																							, ' : ' ,'__')
																						,'','')
																					,'?','')
																				,'(','_')
																			,'\','_')
																		,'/','_')	
																	,')','')
																,' ','')
															,'[','')
														,']','')
													,'{','')
												,'}','')
											,'>','')
										,'<','')
									,',','')
								,'®','')
							,'%','')
						,'=','')
					,'&','_')
					)
END
GO




CREATE PROCEDURE [Graphite].[exec_sp] 
	@sp_name	VARCHAR(200) = 'no_define'
	,@prefix	VARCHAR(100) = NULL
	,@ha		VARCHAR(40)  = NULL
AS
BEGIN
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	BEGIN TRY
		DECLARE @sql VARCHAR(2000)
		
		IF (@prefix IS NULL) OR (@ha IS NULL)
			BEGIN
				PRINT 'Cannot find @Prefix and @ha value from extended property in the master database. Please configue and try again.'
				SELECT Graphite.remove_special_characters('graphite_collector_error.'+@@SERVERNAME +'.'+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+'.prefix_ha_no_define'), 1, GETDATE()
				RETURN 
			END 
			
		IF OBJECT_ID(@sp_name) IS NOT NULL 
		BEGIN
			SET @sql = @sp_name+' '''+@prefix+''','''+@ha+''''
				EXEC (@sql)
						
		END
		ELSE 
			SELECT Graphite.remove_special_characters('graphite_collector_error.'+@@SERVERNAME +'.'+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+'.could_not_found_sp.'+@sp_name), 1, GETDATE()
	END TRY
	BEGIN CATCH
		PRINT 'Store procedure '+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+' has error.' 
		PRINT 'Error : ' + CAST(@@ERROR AS VARCHAR(10)) +' - '+ ERROR_MESSAGE()
		SELECT Graphite.remove_special_characters('graphite_collector_error.'+@@SERVERNAME +'.'+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+'.sp_has_error'), 1, GETDATE()
	END CATCH
END

GO

CREATE PROCEDURE [Graphite].[Collector] 
AS
BEGIN
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	BEGIN TRY
		DECLARE @prefix		VARCHAR(100)	= NULL
				,@ha		VARCHAR(40)		= NULL


		-------------------------------------------------------------------------------------------
		-- Get Prefix or HA
		-------------------------------------------------------------------------------------------
		SELECT @prefix = CONVERT(VARCHAR(100),value)
		FROM master.sys.fn_listextendedproperty ('Graphite_Prefix', default, default, default, default, default, default);

		SELECT @ha = CONVERT(VARCHAR(100),value)
		FROM master.sys.fn_listextendedproperty ('Graphite_ha', default, default, default, default, default, default);
		
		IF (@prefix IS NULL) OR (@ha IS NULL)
		BEGIN
			PRINT 'Cannot find @Prefix and @ha value from extended property in the master database. Please configue and try again.'
			SELECT Graphite.remove_special_characters('graphite_collector_error.'+@@SERVERNAME +'.'+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+'.prefix_ha_no_define'), 1, GETDATE()
			RETURN 
		END 
		-------------------------------------------------------------------------------------------
		-- Call child SP 
		-------------------------------------------------------------------------------------------
			EXEC Graphite.exec_sp 'Graphite.cpu_usage', @prefix, @ha
			EXEC Graphite.exec_sp 'Graphite.Performance_Counter', @prefix, @ha
			EXEC Graphite.exec_sp 'Graphite.Server_Wait_percentage', @prefix, @ha 
			EXEC Graphite.exec_sp 'Graphite.sp_info', @prefix, @ha 
	END TRY
	BEGIN CATCH
		PRINT 'Store procedure '+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+' has error.' 
		PRINT 'Error : ' + CAST(@@ERROR AS VARCHAR(10)) +' - '+ ERROR_MESSAGE()
		SELECT Graphite.remove_special_characters('graphite_collector_error.'+@@SERVERNAME +'.'+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+'.sp_has_error'), 1, GETDATE()
	END CATCH
END

GO

-- EXEC [Graphite].[cpu_usage] @prefix = 'prefix.' , @ha= 'ha.'
CREATE PROCEDURE [Graphite].[cpu_usage]
	@prefix		VARCHAR(100) = NULL
	,@ha		VARCHAR(40) = NULL
AS
BEGIN
	SET NOCOUNT ON
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

	DECLARE @percent	int
			,@servername varchar(200) = @@SERVERNAME
	IF (@prefix IS NULL) OR (@ha IS NULL)
			BEGIN
				PRINT 'Cannot find @Prefix and @ha value from extended property in the master database. Please configue and try again.'
				SELECT Graphite.remove_special_characters('graphite_collector_error.'+@servername +'.'+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+'.prefix_ha_no_define'), 1, GETDATE()
				RETURN 
			END 

	BEGIN TRY	
		BEGIN 
		   DECLARE @SQLSvcUtilization INTEGER = 0
		   ,@OtherOSProcessUtilization INTEGER = 0;

		   WITH CPU
		   AS (
				  SELECT TOP 1 SQLSvcUtilization = SQLSvcUtilization
									  ,SystemIdle = SystemIdle
				  FROM (
						 SELECT record.value('(./Record/@id)[1]', 'int') record_id
								,record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') SystemIdle
								,record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') SQLSvcUtilization
						 FROM (
							   SELECT convert(XML, record) record
							   FROM sys.dm_os_ring_buffers
							   WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
									  AND record LIKE '%<SystemHealth>%'
							   ) x
						 ) y
				  ORDER BY record_id DESC
				  )
		   SELECT @SQLSvcUtilization = SQLSvcUtilization
				  ,@OtherOSProcessUtilization = (100 - SystemIdle - SQLSvcUtilization)
		   FROM CPU


		   SELECT Graphite.remove_special_characters(@prefix + LOWER(@servername)+'.dbengine.mssql.'+@ha+'cpu_usage.sql_utilization')
						 ,@SQLSvcUtilization
						 ,getdate()

		   select Graphite.remove_special_characters(@prefix + LOWER(@servername)+'.dbengine.mssql.'+@ha+'cpu_usage.Other_utilization')
						 ,@OtherOSProcessUtilization
						 ,getdate()

		END
	END TRY
	BEGIN CATCH
		PRINT 'Store procedure '+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+' has error.' 
		PRINT 'Error : ' + CAST(@@ERROR AS VARCHAR(10)) +' - '+ ERROR_MESSAGE()
		SELECT Graphite.remove_special_characters('graphite_collector_error.'+@servername +'.'+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+'.sp_has_error'), 1, GETDATE()
	END CATCH

END

GO

-- EXEC [Graphite].[Performance_Counter] @prefix = 'prefix.' , @ha= 'ha.'
CREATE PROCEDURE [Graphite].[Performance_Counter]
	@prefix		VARCHAR(100) = NULL
	,@ha		VARCHAR(40) = NULL
AS
BEGIN
	SET NOCOUNT ON
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
	BEGIN TRY

		DECLARE @servername varchar(200) = @@SERVERNAME
		IF (@prefix IS NULL) OR (@ha IS NULL)
			BEGIN
				PRINT 'Cannot find @Prefix and @ha value from extended property in the master database. Please configue and try again.'
				SELECT Graphite.remove_special_characters('graphite_collector_error.'+@servername +'.'+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+'.prefix_ha_no_define'), 1, GETDATE()
				RETURN 
			END 
		
	IF OBJECT_ID (N'tempdb.dbo.Performance_counter__response_time__point_A', N'U') IS NULL 
	BEGIN
		SELECT SUM(
				CASE 
					WHEN instance_name = 'Elapsed Time:Requests' THEN cntr_value
					ELSE 0 
				END ) AS'elapsed_time_request_count'
			,SUM(CASE 
					WHEN instance_name = 'Elapsed Time:Total(ms)' THEN cntr_value
					ELSE 0 
				END) 'elapsed_time_request_ms'
			,GETDATE() rec_created_when
		INTO tempdb.dbo.Performance_counter__response_time__point_A
		FROM sys.dm_os_performance_counters
		WHERE cntr_value > 0 
		AND object_name = 'SQLServer:Batch Resp Statistics'
		AND instance_name like 'Elapsed Time%'
		AND counter_name NOT LIKE 'Batches >=100000ms                                                                                                              '
	END
	
	SELECT Graphite.remove_special_characters(@prefix + LOWER(@servername)+'.dbengine.mssql.'+ @ha +'performance_counter.'+SUBSTRING(object_name, CHARINDEX(':', object_name) + 1, LEN(object_name))+'.'+REPLACE(counter_name,'/','-' )+'.'+ CASE WHEN 
	instance_name = ' ' THEN '-' ELSE instance_name END  ) AS measurement_name
			,cntr_value AS value ,
		   GETDATE() AS log_time
	FROM sys.dm_os_performance_counters
	WHERE (object_Name LIKE '%Databases%' 
			AND 
			(counter_name = 'Transactions/sec'
				OR counter_name = 'Active Transactions'
				OR counter_name = 'Log Flush Waits/sec'
				OR counter_name = 'Log Flush Waits Time'
				OR counter_name = 'Log Flushes/sec'
				OR counter_name = 'Log Cache Reads/sec'
				OR counter_name = 'Data File(s) Size (KB)'
				OR counter_name = 'Log File(s) Size (KB)'
				OR counter_name = 'Log File(s) Used Size (KB)'
				OR counter_name = 'Write Transactions/sec'
						)                                                                                                            
					)
			OR (object_Name LIKE '%Databases%' 	
					AND instance_name = '_Total')
			OR (object_name like '%General Statistics%' 
					AND counter_name not like 'SOAP%' 
					AND counter_name not like 'HTTP%' )
			OR (object_name like '%Locks%' 
				AND instance_name = '_Total')
			OR object_name like '%Memory Manager%'
			OR object_name like '%Batch Resp Statistics%'
			OR object_name like '%SQL Statistics%'
			OR object_name like '%Availability Replica%'
			OR (object_name like '%Database Replica%' AND instance_name in ('_Total','Agoda_Core','Agoda_YCS','Agoda_Archive','Agoda_Dragonfruit'))
			OR (object_name like '%SQLServer:Buffer Manager%' AND counter_name like '%Page life expectancy%')
		

	IF OBJECT_ID (N'tempdb.dbo.Performance_counter__response_time__point_B', N'U') IS NOT NULL 
	BEGIN
		DROP TABLE tempdb.dbo.Performance_counter__response_time__point_B 
	END

	SELECT SUM(
			CASE 
				WHEN instance_name = 'Elapsed Time:Requests' THEN cntr_value
				ELSE 0 
			END ) AS'elapsed_time_request_count'
		,SUM(CASE 
				WHEN instance_name = 'Elapsed Time:Total(ms)' THEN cntr_value
				ELSE 0 
			END) 'elapsed_time_request_ms'
		,GETDATE() rec_created_when
	INTO tempdb.dbo.Performance_counter__response_time__point_B
	FROM sys.dm_os_performance_counters
	WHERE cntr_value > 0 
	AND object_name LIKE '%:Batch Resp Statistics%'
	AND instance_name LIKE 'Elapsed Time%'

	SELECT Graphite.remove_special_characters(@prefix + LOWER(@servername)+'.dbengine.mssql.'+ @ha +'performance_counter.batchrespstatistics.avg_response_time_ms') AS measurement_name
			,((b.elapsed_time_request_ms- a.elapsed_time_request_ms)*1.0 / (b.elapsed_time_request_count - a.elapsed_time_request_count)) AS value 
			,GETDATE() AS log_time	
	FROM tempdb.dbo.Performance_counter__response_time__point_A a
		CROSS JOIN tempdb.dbo.Performance_counter__response_time__point_B b


	DROP TABLE tempdb.dbo.Performance_counter__response_time__point_A
	
	SELECT *
	INTO  tempdb.dbo.Performance_counter__response_time__point_A
	FROM tempdb.dbo.Performance_counter__response_time__point_B

	DROP TABLE tempdb.dbo.Performance_counter__response_time__point_B
	
	END TRY
	BEGIN CATCH
		PRINT 'Store procedure '+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+' has error.' 
		PRINT 'Error : ' + CAST(@@ERROR AS VARCHAR(10)) +' - '+ ERROR_MESSAGE()
		SELECT Graphite.remove_special_characters('graphite_collector_error.'+@servername +'.'+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+'.sp_has_error'), 1, GETDATE()
	END CATCH
END

GO


-- EXEC [Graphite].[Server_Wait_percentage] @prefix = 'prefix.' , @ha= 'ha.'
CREATE PROCEDURE [Graphite].[Server_Wait_percentage]
	@prefix		VARCHAR(100) = NULL
	,@ha		VARCHAR(40) = NULL
AS
BEGIN
	SET NOCOUNT ON
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
	BEGIN TRY
	
	DECLARE @servername varchar(200) = @@SERVERNAME

	IF (@prefix IS NULL) OR (@ha IS NULL)
			BEGIN
				PRINT 'Cannot find @Prefix and @ha value from extended property in the master database. Please configue and try again.'
				SELECT Graphite.remove_special_characters('graphite_collector_error.'+@servername +'.'+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+'.prefix_ha_no_define'), 1, GETDATE()
				RETURN 
			END 

	SELECT
	        [wait_type],
	        [wait_time_ms]  AS [WaitS],
	        [waiting_tasks_count] AS [WaitCount],
	        100.0 * [wait_time_ms] / SUM ([wait_time_ms]) OVER() AS [Percentage],
	        ROW_NUMBER() OVER(ORDER BY [wait_time_ms] DESC) AS [RowNum]
		INTO #A
	    FROM sys.dm_os_wait_stats
	    WHERE [wait_type] NOT IN (
	        N'BROKER_EVENTHANDLER', N'BROKER_RECEIVE_WAITFOR',
	        N'BROKER_TASK_STOP', N'BROKER_TO_FLUSH',
	        N'BROKER_TRANSMITTER', N'CHECKPOINT_QUEUE',
	        N'CHKPT', N'CLR_AUTO_EVENT',
	        N'CLR_MANUAL_EVENT', N'CLR_SEMAPHORE',
	
	       -- -- Maybe uncomment these four if you have mirroring issues
	       -- N'DBMIRROR_DBM_EVENT', N'DBMIRROR_EVENTS_QUEUE',
	       -- N'DBMIRROR_WORKER_QUEUE', N'DBMIRRORING_CMD',
		   --
	       -- N'DIRTY_PAGE_POLL', N'DISPATCHER_QUEUE_SEMAPHORE',
	       -- N'EXECSYNC', N'FSAGENT',
	       -- N'FT_IFTS_SCHEDULER_IDLE_WAIT', N'FT_IFTSHC_MUTEX',
		   --
	       -- -- Maybe uncomment these six if you have AG issues
	       -- N'HADR_CLUSAPI_CALL', N'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
	       -- N'HADR_LOGCAPTURE_WAIT', N'HADR_NOTIFICATION_DEQUEUE',
	       -- N'HADR_TIMER_TASK', N'HADR_WORK_QUEUE',
	
	        N'KSOURCE_WAKEUP', N'LAZYWRITER_SLEEP',
	        N'LOGMGR_QUEUE', N'MEMORY_ALLOCATION_EXT',
	        N'ONDEMAND_TASK_QUEUE',
	        N'PREEMPTIVE_XE_GETTARGETSTATE',
	        N'PWAIT_ALL_COMPONENTS_INITIALIZED',
	        N'PWAIT_DIRECTLOGCONSUMER_GETNEXT',
	        N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', N'QDS_ASYNC_QUEUE',
	        N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
	        N'QDS_SHUTDOWN_QUEUE', N'REDO_THREAD_PENDING_WORK',
	        N'REQUEST_FOR_DEADLOCK_SEARCH', N'RESOURCE_QUEUE',
	        N'SERVER_IDLE_CHECK', N'SLEEP_BPOOL_FLUSH',
	        N'SLEEP_DBSTARTUP', N'SLEEP_DCOMSTARTUP',
	        N'SLEEP_MASTERDBREADY', N'SLEEP_MASTERMDREADY',
	        N'SLEEP_MASTERUPGRADED', N'SLEEP_MSDBSTARTUP',
	        N'SLEEP_SYSTEMTASK', N'SLEEP_TASK',
	        N'SLEEP_TEMPDBSTARTUP', N'SNI_HTTP_ACCEPT',
	        N'SP_SERVER_DIAGNOSTICS_SLEEP', N'SQLTRACE_BUFFER_FLUSH',
	        N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
	        N'SQLTRACE_WAIT_ENTRIES', N'WAIT_FOR_RESULTS',
	        N'WAITFOR', N'WAITFOR_TASKSHUTDOWN',
	        N'WAIT_XTP_RECOVERY',
	        N'WAIT_XTP_HOST_WAIT', N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG',
	        N'WAIT_XTP_CKPT_CLOSE', N'XE_DISPATCHER_JOIN',
	        N'XE_DISPATCHER_WAIT', N'XE_TIMER_EVENT'
			,N'HADR_WORK_QUEUE',N'HADR_NOTIFICATION_DEQUEUE',N'HADR_TIMER_TASK'
			,N'HADR_FILESTREAM_IOMGR_IOCOMPLETION'
			,N'DIRTY_PAGE_POLL'
			,N'FT_IFTS_SCHEDULER_IDLE_WAIT'
			,N'DISPATCHER_QUEUE_SEMAPHORE'
			,N'UCS_SESSION_REGISTRATION'
			,N'HADR_NOTIFICATION_DEQUEUE'
			,N'HADR_TIMER_TASK'
			)

		AND [wait_type] not like 'XE_%'
		AND [wait_type] not like 'preemptive_xe%'
	    AND [waiting_tasks_count] > 0
	   
		
	SELECT
	 MAX ([W1].[wait_type]) AS [Wait_Type],
	    CAST (MAX ([W1].[WaitS]) AS DECIMAL (16,2)) AS [Wait_time],
	    MAX ([W1].[WaitCount]) AS [WaitCount]
	INTO #B
	FROM #A AS [W1]
	INNER JOIN #A AS [W2]
	    ON [W2].[RowNum] <= [W1].[RowNum]
	WHERE [W1].[wait_type] not like '%HADR%'
	AND [W1].[wait_type] not like '%THREADPOOL%'
	AND [W1].[wait_type] not like '%LCK%'
	GROUP BY [W1].[RowNum]
	HAVING SUM ([W2].[Percentage]) - MAX( [W1].[Percentage] ) < 99

	UNION ALL

	SELECT #A.[wait_type],#A.[WaitS],#A.[WaitCount]
	FROM #A
	WHERE #A.[wait_type] like '%HADR%'
	OR #A.[wait_type] like '%THREADPOOL%'
	OR #A.[wait_type] like '%LCK%'


	SELECT Graphite.remove_special_characters(@prefix + LOWER(@servername)+'.dbengine.mssql.'+ @ha +'wait_stats.'+wait_type+'.wait_time_ms') AS measurement_name
		   ,[Wait_time] as count ,
		   GETDATE() rec_created
	FROM #B
	
	UNION ALL
	
	SELECT Graphite.remove_special_characters(@prefix + LOWER(@servername)+'.dbengine.mssql.'+ @ha +'wait_stats.'+wait_type+'.wait_count') AS measurement_name
		,[WaitCount] AS TIME
		,GETDATE() rec_created
	FROM #B

	END TRY
		BEGIN CATCH
			PRINT 'Store procedure '+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+' has error.' 
			PRINT 'Error : ' + CAST(@@ERROR AS VARCHAR(10)) +' - '+ ERROR_MESSAGE()
			SELECT Graphite.remove_special_characters('graphite_collector_error.'+@servername +'.'+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+'.sp_has_error'), 1, GETDATE()
		END CATCH
END
GO

-- EXEC [Graphite].[sp_info] @prefix = 'prefix.' , @ha= 'ha.'
CREATE PROCEDURE [Graphite].[sp_info]  
	@prefix		VARCHAR(100) = NULL
	,@ha		VARCHAR(40) = NULL
AS
BEGIN

	SET NOCOUNT ON ;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	
	-- Prefix --
	
	DECLARE  @from_date				SMALLDATETIME 
			,@to_date				SMALLDATETIME

	DECLARE @NodeName varchar(100) = 'event'
	BEGIN TRY
		DECLARE	@databasename VARCHAR(100),
				@query VARCHAR(MAX),
				@query2 VARCHAR(MAX),
				@servername varchar(200) = @@SERVERNAME


		IF (@prefix IS NULL) OR (@ha IS NULL)
			BEGIN
				PRINT 'Cannot find @Prefix and @ha value from extended property in the master database. Please configue and try again.'
				SELECT Graphite.remove_special_characters('graphite_collector_error.'+@servername +'.'+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+'.prefix_ha_no_define'), 1, GETDATE()
				RETURN 
			END 	

	
	SELECT OBJECT_SCHEMA_NAME ( object_id , database_id  ) as schemas ,db_name(database_id) as database_name , object_name (object_id, database_id) as object_name ,
	database_id,	object_iD,	cached_time,	execution_count,	total_worker_time,	last_worker_time,	total_physical_reads,	last_physical_reads,	total_logical_writes,
	last_logical_writes,	total_logical_reads,	last_logical_reads,	total_elapsed_time,	last_elapsed_time

	INTO #A
	FROM  sys.dm_exec_procedure_stats
	WHERE object_name (object_id, database_id) NOT LIKE 'sp_ms%'
	AND db_name(database_id) NOT LIKE 'model'
	AND db_name(database_id) NOT LIKE 'msdb'
	AND db_name(database_id) NOT LIKE 'tempdb'
	AND db_name(database_id) NOT LIKE 'distribution'
	--AND OBJECT_SCHEMA_NAME ( object_id , database_id  ) NOT LIKE '%Graphite%'
	AND last_execution_time > DATEADD(MINUTE,-5,GETDATE())

	-- Select measurement to graphite --

	SELECT  Graphite.remove_special_characters(@prefix + @servername+'.dbengine.mssql.'+@ha+'sp_info.database.'+database_name+'.schema.'+ schemas +'.sp_name.'  + object_name+'.execution_count' ) AS measurement_name
			, execution_count				AS measurement_value
			, GETDATE()						AS last_update
	FROM #A
	UNION ALL
	SELECT  Graphite.remove_special_characters(@prefix + @servername+'.dbengine.mssql.'+@ha+'sp_info.database.'+database_name+'.schema.'+ schemas +'.sp_name.'  + object_name+'.total_worker_time')
			, total_worker_time
			, GETDATE() AS last_update
	FROM #A
	UNION ALL
	SELECT  Graphite.remove_special_characters(@prefix + @servername+'.dbengine.mssql.'+@ha+'sp_info.database.'+database_name+'.schema.'+ schemas +'.sp_name.'  + object_name+'.total_physical_reads')
			, total_physical_reads
			, GETDATE() AS last_update
	FROM #A
	UNION ALL
	SELECT  Graphite.remove_special_characters(@prefix + @servername+'.dbengine.mssql.'+@ha+'sp_info.database.'+database_name+'.schema.'+ schemas +'.sp_name.'  + object_name+'.total_logical_writes')
			, total_logical_writes
			, GETDATE() AS last_update
	FROM #A
	UNION ALL
	SELECT  Graphite.remove_special_characters(@prefix + @servername+'.dbengine.mssql.'+@ha+'sp_info.database.'+database_name+'.schema.'+ schemas +'.sp_name.'  + object_name+'.total_logical_reads')
			, total_logical_reads
			, GETDATE() AS last_update
	FROM #A
	UNION ALL
	SELECT  Graphite.remove_special_characters(@prefix + @servername+'.dbengine.mssql.'+@ha+'sp_info.database.'+database_name+'.schema.'+ schemas +'.sp_name.'  + object_name+'.total_elapsed_time')
			, total_elapsed_time
			, GETDATE() AS last_update
	FROM #A

	-- CACHE TIME --
	UNION ALL
	SELECT  Graphite.remove_special_characters(@prefix + @servername+'.dbengine.mssql.'+@ha+'sp_info.database.'+database_name+'.schema.'+ schemas +'.sp_name.'  + object_name+'.time_in_cached_min')
			, datediff(mi,cached_time,getdate())
			, GETDATE() AS last_update
	FROM #A
	END TRY
	BEGIN CATCH
		PRINT 'Store procedure '+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+' has error.' 
		PRINT 'Error : ' + CAST(@@ERROR AS VARCHAR(10)) +' - '+ ERROR_MESSAGE()
		SELECT Graphite.remove_special_characters('graphite_collector_error.'+@servername +'.'+OBJECT_SCHEMA_NAME(@@PROCID)+'.'+OBJECT_NAME(@@PROCID)+'.sp_has_error'), 1, GETDATE()
	END CATCH

END

GO

















