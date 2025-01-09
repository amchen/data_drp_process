set ansi_padding, ansi_warnings, concat_null_yields_null, arithabort, quoted_identifier, ansi_nulls on;
go
set lock_timeout 3000;
go
if object_id('dba_util.drp_conversion_delete','P') is not null
begin
    set noexec on;
end
go
create procedure dba_util.drp_conversion_delete as select 1 as one
go
set noexec off;
go
/*
-- =============================================
-- Author:      Andrew Chen
-- Create date: 20231218
-- Description: Delete NON Lead conversions (based off dba_util.drp_generic_delete procedure)

-- 20231218, Andrew Chen, Discovery - Conversion
-- =============================================
level_id = 1, frequent access (primary db)
level_id = 2, infrequent access (archive db)
level_id = 3, offline archive (S3 data)

level_mode = 0, Off
level_mode = 1, Move to next level
level_mode = 2, Delete
*/
alter procedure [dba_util].[drp_conversion_delete]
    @level_id int = 0,
    @level_mode  tinyint = 0,
    @drp_table_id int = 0,
    @parameter_xml nvarchar(max) = NULL,
    @debug_only bit = 1
as
begin
    set nocount on;
    set transaction isolation level read uncommitted;

    declare @msg varchar(max);
    declare @proc_name varchar(255) = object_name(@@procid);

    -- sanity check level
    if (@level_id = 0 or @level_mode = 0 or @drp_table_id = 0)
    begin
        set @msg = '@level_id, @level_mode, @drp_table_id cannot be 0';
        exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
        return;
    end

    -- sanity check parameter_xml
    if (@parameter_xml is NULL or len(@parameter_xml) < 5)
    begin
        set @msg = '@parameter_xml cannot be empty';
        exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
        return;
    end

    -- =============================================
    -- Parameter XML
    -- =============================================
    declare @parameter_error int;
    declare @parameter_error_message varchar(1000);

    if object_id('tempdb..#parameter_table') is not null drop table #parameter_table;
    create table #parameter_table
    (
        entity_type_id tinyint
        ,name varchar(100)
        ,raw_value nvarchar(max)
        ,value_int int null
        ,value_decimal decimal(28, 18) null
        ,value_bool bit null
        ,value_date datetime null
        ,value_string nvarchar(max) null
    );
    create index IX_parameter_table on #parameter_table (name) include (value_string);

    exec dba_util.drp_parameterxml_parse
        @parameter_xml = @parameter_xml
        ,@parameter_error = @parameter_error output
        ,@parameter_error_message = @parameter_error_message output
        ;

    -- error in parameter_xml
    if (@parameter_error <> 0)
    begin
        exec dba.dba_util.print_log @log_msg = @parameter_error_message, @procedure_name = @proc_name;
        return;
    end
    --select * from #parameter_table;

    declare @sqlselect nvarchar(max);
    declare @sqldelete varchar(max);
    declare @qry varchar(max);
    declare @cnt int;
    declare @chunk_size int;
    declare @min_id bigint;
    declare @max_id bigint;
    declare @table_min_date datetime;
    declare @drp_min_date datetime;
    declare @drp_max_date datetime;
    declare @drp_work_spid bigint;
    declare @sqls3 varchar(max);
    declare @dt_path varchar(100);
    declare @table_name varchar(100);

    -- get status
    select @drp_min_date = drp_min_date from dba_util.drp_status where drp_table_id = @drp_table_id;
    select @table_name = table_name from dba_util.drp_table where drp_table_id = @drp_table_id;

    -- always get min_date from table
    set @sqlselect = 'select @min_date = min(@column_name_date) from [@db_name].@db_schema.@table_name with (nolock) where @column_name_id > 0 and lead_id is NULL';
    select @sqlselect = replace(@sqlselect, '@' + name, value_string) from #parameter_table;
    set @msg = 'Check status @sqlselect: ' + @sqlselect;
    exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
    exec sp_executesql @sqlselect, N'@min_date datetime OUTPUT', @min_date = @table_min_date OUTPUT;
    set @table_min_date = dateadd(hour, datediff(hour, 0, @table_min_date), 0);

    -- sanity check table
    if (@table_min_date is null)
    begin
        set @msg = '*** Table is empty: ' + @table_name + ' ***';
        exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
        return;
    end

    -- if first time processing table
    if (@drp_min_date is null)
    begin
        if (@debug_only <> 1)
        begin
            insert dba_util.drp_status
            select
                @drp_table_id
                ,@table_min_date
                ,table_name
                ,column_name_id
                ,column_name_date
                ,repository_provider
                ,dbo.GetInstanceDate(NULL)
                ,@parameter_xml
                from dba_util.drp_table
                where drp_table_id = @drp_table_id
                ;
        end
    end

    -- use min_date from table
    -- this allows for skipping time blocks with no data and also recovers from deletion errors
    set @drp_min_date = @table_min_date;
    -- get next hour date range
    select @drp_max_date = dateadd(hour, 1, @drp_min_date);
    -- select query
    select @drp_work_spid = @@spid;
    -- sql with column_name_id
    set @sqlselect = 'insert [@db_primary].dba_util.drp_work_spid select top(@chunk_size) @drp_work_spid as drp_work_spid, 0 as drp_step_id, ' +
                     '''@drp_min_date'' as drp_min_date, ''@drp_max_date'' as drp_max_date, @column_name_id as column_id_value ' +
                     'from [@db_name].@db_schema.@table_name ' +
                     'where @column_name_date >= ''@drp_min_date'' and @column_name_date < ''@drp_max_date'' and @column_name_date < ''@date_drp'' ' +
                     'and @column_name_id > 0 and lead_id is NULL ' +
                     'order by @column_name_id';

    select @sqlselect = replace(@sqlselect, '@' + name, value_string) from #parameter_table;
    select @sqlselect = replace(@sqlselect, '@drp_work_spid', cast(@drp_work_spid as varchar(20)));
    select @sqlselect = replace(@sqlselect, '@drp_min_date', convert(varchar, @drp_min_date, 120));
    select @sqlselect = replace(@sqlselect, '@drp_max_date', convert(varchar, @drp_max_date, 120));

    -- check all parameters were provided
    if (charindex('@', @sqlselect) > 0)
    begin
        set @msg = 'Variable not provided: ' + @sqlselect;
        exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
        return;
    end

    -- select data chunk
    exec dba.dba_util.print_log @log_msg = @sqlselect, @procedure_name = @proc_name;
    exec(@sqlselect);

    -- Step 0, init
    select @cnt = count(*) from dba_util.drp_work_spid where drp_work_spid = @drp_work_spid and drp_step_id = 0;
    select @chunk_size = cast(value_string as int) from #parameter_table where name = 'chunk_size';
    if (@cnt = 0)
    begin
        set @msg = '*** No data to delete ***';
        exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;

        if (@debug_only <> 1)
        begin
            -- update status
            update dba_util.drp_status
                set drp_min_date = @drp_max_date, parameter_xml = @parameter_xml, date_updated = dbo.GetInstanceDate(NULL)
                where drp_table_id = @drp_table_id
                ;
        end
        return;
    end

    -- s3 query
    select @dt_path = 'dt='+replace(replace(convert(varchar, @drp_min_date, 120), ' ', '-'), ':', '-');
    select @table_name = value_string from #parameter_table where name = 'table_name';

    set @sqls3 = 'select t.* from [@db_name].@db_schema.@table_name t ' +
                 'inner join [@db_primary].dba_util.drp_work_spid w on t.@column_name_id = w.column_id_value ' +
                 'and t.@column_name_date >= w.drp_min_date and t.@column_name_date < w.drp_max_date ' +
                 'and t.@column_name_id > 0 ' +
                 'and w.drp_work_spid = @drp_work_spid and w.drp_step_id = 1';

    select @sqls3 = replace(@sqls3, '@' + name, value_string) from #parameter_table;
    select @sqls3 = replace(@sqls3, '@drp_work_spid', cast(@drp_work_spid as varchar(20)));

    -- check all parameters were provided
    if (charindex('@', @sqls3) > 0)
    begin
        set @msg = 'Variable not provided: ' + @sqls3;
        exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
        return;
    end

    -- delete query
    set @sqldelete = 'delete t from [@db_name].@db_schema.@table_name t ' +
                     'inner join [@db_primary].dba_util.drp_work_spid w on w.column_id_value = t.@column_name_id ' +
                     'and t.@column_name_date >= w.drp_min_date and t.@column_name_date < w.drp_max_date ' +
                     'and t.@column_name_id > 0 ' +
                     'and w.drp_work_spid = @drp_work_spid and w.drp_step_id = 2';

    select @sqldelete = replace(@sqldelete, '@' + name, value_string) from #parameter_table;
    select @sqldelete = replace(@sqldelete, '@drp_work_spid', cast(@drp_work_spid as varchar(20)));
    
    -- check all parameters were provided
    if (charindex('@', @sqldelete) > 0)
    begin
        set @msg = 'Variable not provided: ' + @sqldelete;
        exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
        return;
    end

    begin try
        -- Step 1, s3 export
        update dba_util.drp_work_spid
            set drp_step_id = 1
            where drp_work_spid = @drp_work_spid
              and drp_step_id = 0
              ;

        if (@debug_only = 0)
        begin
            -- stop circular FK reference between conversions_disposition_history and conversions tables
            update c set current_disposition_history_id = NULL
                from conversions c inner join dba_util.drp_work_spid s on c.conversion_id = s.column_id_value and s.drp_step_id = 1
                where c.lead_id is null;
        end

        -- loop through drp_table_tree
        declare @column_name_id varchar(50);
        declare @sqls3tree varchar(max);
        declare @sqldeletetree varchar(max);

        declare drpTableTree cursor local fast_forward for
            select table_name, column_name_id
                from dba_util.drp_table_tree with (nolock)
                where (drp_table_id = @drp_table_id and is_active = 1)
                order by order_id asc
                ;
        open drpTableTree;
        fetch next from drpTableTree into @table_name, @column_name_id;
            while @@fetch_status = 0
            begin
                -- s3 query
                set @sqls3tree = 'select t.* from [@db_name].@db_schema.@table_name t ' +
                             'inner join [@db_primary].dba_util.drp_work_spid w on t.@column_name_id = w.column_id_value ' +
                             'and t.@column_name_id > 0 ' +
                             'and w.drp_work_spid = @drp_work_spid and w.drp_step_id = 1';
                select @sqls3tree = replace(@sqls3tree, '@table_name', @table_name);
                select @sqls3tree = replace(@sqls3tree, '@column_name_id', @column_name_id);
                select @sqls3tree = replace(@sqls3tree, '@' + name, value_string) from #parameter_table;
                select @sqls3tree = replace(@sqls3tree, '@drp_work_spid', cast(@drp_work_spid as varchar(20)));

                -- check all parameters were provided
                if (charindex('@', @sqls3tree) > 0)
                begin
                    set @msg = 'Variable not provided: ' + @sqls3tree;
                    exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
                    return;
                end

                -- delete query
                set @sqldeletetree = 'delete t from [@db_name].@db_schema.@table_name t ' +
                                 'inner join [@db_primary].dba_util.drp_work_spid w on w.column_id_value = t.@column_name_id ' +
                                 'and t.@column_name_id > 0 ' +
                                 'and w.drp_work_spid = @drp_work_spid and w.drp_step_id = 1';
                select @sqldeletetree = replace(@sqldeletetree, '@table_name', @table_name);
                select @sqldeletetree = replace(@sqldeletetree, '@column_name_id', @column_name_id);
                select @sqldeletetree = replace(@sqldeletetree, '@' + name, value_string) from #parameter_table;
                select @sqldeletetree = replace(@sqldeletetree, '@drp_work_spid', cast(@drp_work_spid as varchar(20)));
    
                -- check all parameters were provided
                if (charindex('@', @sqldeletetree) > 0)
                begin
                    set @msg = 'Variable not provided: ' + @sqldeletetree;
                    exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
                    return;
                end

                -- check level and mode
                if (@level_id = 1 and @level_mode = 1)
                begin
                    exec dba.dba_util.print_log @log_msg = @sqls3tree, @procedure_name = @proc_name;
                    exec dba_util.drp_generic_copy_s3 @sqls3tree, @dt_path, @table_name, @debug_only;
                end

                -- delete drp_table_tree
                exec dba.dba_util.print_log @log_msg = @sqldeletetree, @procedure_name = @proc_name;
                if (@debug_only = 0) exec(@sqldeletetree);

                fetch next from drpTableTree into @table_name, @column_name_id;
            end
        close drpTableTree;
        deallocate drpTableTree;

        -- Step 2, delete
        update dba_util.drp_work_spid
            set drp_step_id = 2
            where drp_work_spid = @drp_work_spid
              and drp_step_id = 1
              ;

        -- delete drp table
        exec dba.dba_util.print_log @log_msg = @sqldelete, @procedure_name = @proc_name;
        if (@debug_only = 0)
        begin
            -- stop circular FK reference between conversions_disposition_history and conversions tables
            update c set current_disposition_history_id = NULL
                from conversions c inner join dba_util.drp_work_spid s on c.conversion_id = s.column_id_value and s.drp_step_id = 2
                where c.lead_id is null;

            exec(@sqldelete);
        end

        if (@debug_only <> 1)
        begin
            -- update status and min_date
            -- if cnt = chunk_size, there are still more records in the hour block, do not increment the min_date
            -- else increment min_date to max_date
            update dba_util.drp_status
                set drp_min_date = case when @cnt = @chunk_size then @drp_min_date else @drp_max_date end, date_updated = dbo.GetInstanceDate(NULL)
                where drp_table_id = @drp_table_id
                ;
        end

        -- cleanup drp_work_spid
        delete dba_util.drp_work_spid
            where drp_work_spid = @drp_work_spid
              and drp_step_id = 2;
    end try
    begin catch
        select * from dba_util.drp_work_spid;

        if @@trancount > 0
            rollback;

        declare @errormessage nvarchar(4000);
        declare @errorseverity int;
        declare @errorstate int;

        select
            @errormessage = error_message(),
            @errorseverity = error_severity(),
            @errorstate = error_state();

        -- cleanup drp_work_spid
        delete dba_util.drp_work_spid
            where drp_work_spid = @drp_work_spid;

        -- use raiserror inside the catch block to return information about the original error
        raiserror (@errormessage,    -- message text
                   @errorseverity,    -- severity
                   @errorstate        -- state
                   );
    end catch
end
go
