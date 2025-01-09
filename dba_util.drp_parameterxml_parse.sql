set ansi_padding, ansi_warnings, concat_null_yields_null, arithabort, quoted_identifier, ansi_nulls on;
go
set lock_timeout 3000;
go
if object_id('dba_util.drp_parameterxml_parse','P') is not null
begin
    set noexec on;
end
go
create procedure dba_util.drp_parameterxml_parse as select 1 as one
go
set noexec off;
go
-- =============================================
-- Author:      Andrew Chen
-- Create date: 20190910
-- Description: Parse data retention policy parameter_xml

-- 20190910, Andrew Chen, Data Retention Purge/Archival
-- 20231128, Andrew Chen, Discovery
-- =============================================
alter procedure [dba_util].[drp_parameterxml_parse]
    @parameter_xml nvarchar(max)
    ,@parameter_error int output
    ,@parameter_error_message varchar(1000) output
as
begin
    set nocount on;
    set transaction isolation level read uncommitted;

    set @parameter_error = 0;
    set @parameter_error_message = '';

    -- parse xml into variable
    begin try
        declare @xml xml = @parameter_xml;

        -- using value_string since these are used as dynamic sql
        ;with parm as
        (
            select 
                t.c.value('../@name', 'varchar(100)') as name
                ,t.c.value('.', 'nvarchar(max)') as value
            from @xml.nodes('//parameter/value') t(c)
        )
        insert #parameter_table (name, value_string)
        select 
            parm.name
            ,parm.value
        from parm;
    end try
    begin catch
        set @parameter_error = 1;
        set @parameter_error_message = object_name(@@procid) + ': parameter_xml malformed; check message';
    end catch
end
go
