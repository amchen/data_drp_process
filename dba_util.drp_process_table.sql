set ansi_padding, ansi_warnings, concat_null_yields_null, arithabort, quoted_identifier, ansi_nulls on;
go
set lock_timeout 3000;
go
if object_id('dba_util.drp_process_table','P') is not null
begin
    set noexec on;
end
go
create procedure dba_util.drp_process_table as select 1 as one
go
set noexec off;
go
-- =============================================
-- Author:      Andrew Chen
-- Create date: 20190910
-- Description: Processes data retention policy

-- 20190910, Andrew Chen, Data Retention Purge/Archival
-- 20231128, Andrew Chen, Discovery
-- =============================================
alter procedure [dba_util].[drp_process_table]
    @group_id   int = 0,
    @level_id   int = 0,
    @level_mode tinyint = 0,
    @level_time int = 0,
    @debug_only bit = 1    
as
begin
    set nocount on;
    set transaction isolation level read uncommitted;

    -- sanity check
    if (@group_id = 0 or @level_id = 0 or @level_mode = 0)
        return;

    set @level_time = abs(@level_time);
    declare @db_name varchar(50) = db_name();
    declare @msg varchar(max);
    declare @proc_name varchar(255) = object_name(@@procid);
    declare @now datetime = dbo.GetInstanceDate(NULL);
    declare @date_drp datetime = dateadd(hour, -1 * @level_time, @now);
    declare @drp_table_id int;
    declare @table_name varchar(100);
    declare @column_name_id varchar(50);
    declare @column_name_date varchar(50);
    declare @chunk_size int;
    declare @repository_provider varchar(50);
    declare @method varchar(50);
    declare @command varchar(100);
    declare @parameter_xml varchar(max);
    declare @sql varchar(max);
    declare @qry varchar(max);

    -- calculate the drp date (cut off) based on level_time
    if (@level_time < 10)
    begin
        -- time in years
        set @date_drp = dateadd(year, datediff(year, 0, @now) - @level_time, 0);
    end

    -- process drp_table
    declare drpTable cursor local fast_forward for
        select drp_table_id, table_name, column_name_id, column_name_date, chunk_size, repository_provider, method, command, parameter_xml
            from dba_util.drp_table with (nolock)
            where (drp_group_id = @group_id and is_active = 1)
            order by drp_table_id asc
            ;
    open drpTable;
    fetch next from drpTable into @drp_table_id, @table_name, @column_name_id, @column_name_date, @chunk_size, @repository_provider, @method, @command, @parameter_xml;
        while @@fetch_status = 0
        begin
            -- replace variables
            if (len(@parameter_xml) > 5)
            begin
                -- primary database
                if (@level_id = 1)
                begin
                    set @parameter_xml = replace(@parameter_xml, '@@db_name', @db_name);
                    set @parameter_xml = replace(@parameter_xml, '@@db_primary', @db_name);
                    set @parameter_xml = replace(@parameter_xml, '@@db_schema', 'dbo');
                end

                -- archive database
                if (@level_id = 2)
                begin
                    set @parameter_xml = replace(@parameter_xml, '@@db_name', @db_name + '_arch');
                    set @parameter_xml = replace(@parameter_xml, '@@db_primary', @db_name);
                    set @parameter_xml = replace(@parameter_xml, '@@db_schema', 'arch');
                end

                set @parameter_xml = replace(@parameter_xml, '@@table_name', @table_name);
                set @parameter_xml = replace(@parameter_xml, '@@column_name_id', @column_name_id);
                set @parameter_xml = replace(@parameter_xml, '@@column_name_date', @column_name_date);
                set @parameter_xml = replace(@parameter_xml, '@@chunk_size', cast(@chunk_size as varchar(20)));
                set @parameter_xml = replace(@parameter_xml, '@@date_drp', convert(varchar, @date_drp, 120));

                set @msg = '@parameter_xml = ' + @parameter_xml;
                exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
            end
/*
            -- offline S3
            if (@level_id = 3)
            begin
            end
*/
            if (@repository_provider = 'sql' and @method = 'proc')
            begin
                set @qry = 'exec @@command @level_id = @@level_id, @level_mode = @@level_mode, @drp_table_id = @@drp_table_id, @parameter_xml = ''@@parameter_xml'', @debug_only = @@debug_only;';
                set @sql = replace(@qry, '@@command', @command);
                set @sql = replace(@sql, '@@level_id', cast(@level_id as varchar(10)));
                set @sql = replace(@sql, '@@level_mode', cast(@level_mode as varchar(10)));
                set @sql = replace(@sql, '@@drp_table_id', cast(@drp_table_id as varchar(10)));
                set @sql = replace(@sql, '@@parameter_xml', @parameter_xml);
                set @sql = replace(@sql, '@@debug_only', cast(@debug_only as varchar(10)));
                exec dba.dba_util.print_log @log_msg = @sql, @procedure_name = @proc_name;
                exec(@sql);
            end

            fetch next from drpTable into @drp_table_id, @table_name, @column_name_id, @column_name_date, @chunk_size, @repository_provider, @method, @command, @parameter_xml;
        end
    close drpTable;
    deallocate drpTable;

end
go
