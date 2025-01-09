set ansi_padding, ansi_warnings, concat_null_yields_null, arithabort, quoted_identifier, ansi_nulls on;
go
set lock_timeout 3000;
go
if object_id('dba_util.drp_process_group','P') is not null
begin
    set noexec on;
end
go
create procedure dba_util.drp_process_group as select 1 as one
go
set noexec off;
go
/*
-- =============================================
-- Author:      Andrew Chen
-- Create date: 20190910
-- Description: Processes data retention policy

-- 20190910, Andrew Chen, Data Retention Purge/Archival
-- 20231128, Andrew Chen, Discovery
-- =============================================
insert config_key_value_pairs (cake_key,value_bool) values ('drp_enabled',1);

exec dba_util.drp_process_group;
exec dba_util.drp_process_group @group_id = 3;                                  -- group 3, all levels, debug only
exec dba_util.drp_process_group @level_id = 2;                                  -- group 2, all levels, debug only
exec dba_util.drp_process_group @group_id = 4, @level_id = 1;                   -- group 4, level 1, debug only
exec dba_util.drp_process_group @group_id = 4, @level_id = 1, @debug_only = 0;  -- group 4, level 1, execute drp
*/
alter procedure [dba_util].[drp_process_group]
    @group_id int = NULL,
    @level_id int = 0,
    @debug_only bit = 1
as
begin
    set nocount on;
    set transaction isolation level read uncommitted;

    declare @offset_drp int = 0;    -- offset for grace period
    declare @drp_group_id int;
    declare @group_name varchar(100);
    declare @level_mode tinyint;
    declare @level_time int;
    declare @level1_mode tinyint;
    declare @level1_time int;
    declare @level2_mode tinyint;
    declare @level2_time int;
    declare @level3_mode tinyint;
    declare @level3_time int;
    declare @msg varchar(max);
    declare @proc_name varchar(255);

    set @proc_name = object_name(@@procid);

    declare @drp_enabled bit;
    set @drp_enabled = isnull(dbo.ckvp_bool('drp_enabled'), 0);

    -- sanity check
    if (@drp_enabled <> 1)
    begin
        set @msg = '**** ckvp: drp_enabled.value_bool <> 1 ****';
        exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
        return;
    end

    if (@level_id = 0)
    begin
        set @msg = '@level_id must be > 0';
        exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
        return;
    end

    -- variables
    --set @now = dbo.GetInstanceDate(NULL);
    --set @qry = 'exec dbo.drp_generic_delete @parameter_xml = ''@@parameter_xml''';
    
    -- process drp_group
    declare drpGroup cursor local fast_forward for
        select drp_group_id, group_name, level1_mode, level1_time, level2_mode, level2_time, level3_mode, level3_time
            from dba_util.drp_group with (nolock)
            where (@group_id is NULL or drp_group_id = @group_id)
            ;
    open drpGroup;
    fetch next from drpGroup into @drp_group_id, @group_name, @level1_mode, @level1_time, @level2_mode, @level2_time, @level3_mode, @level3_time;
        while @@fetch_status = 0
        begin
            -- primary database
            if (@level_id = 1 and @level1_mode > 0)
            begin
                set @level_mode = @level1_mode;
                set @level_time = @level1_time + @offset_drp;
            end

            -- archive database
            if (@level_id = 2 and @level2_mode > 0)
            begin
                set @level_mode = @level2_mode;
                set @level_time = @level2_time + @offset_drp;
            end

            -- offline S3
            if (@level_id = 3 and @level3_mode > 0)
            begin
                set @level_mode = @level3_mode;
                set @level_time = @level3_time + @offset_drp;
            end

            -- process drp tables in group
            exec dba_util.drp_process_table @group_id = @drp_group_id, @level_id = @level_id, @level_mode = @level_mode, @level_time = @level_time, @debug_only = @debug_only;

            fetch next from drpGroup into @drp_group_id, @group_name, @level1_mode, @level1_time, @level2_mode, @level2_time, @level3_mode, @level3_time;
        end
    close drpGroup;
    deallocate drpGroup;

end
go
