SET ANSI_PADDING, ANSI_WARNINGS, CONCAT_NULL_YIELDS_NULL, ARITHABORT, QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
SET LOCK_TIMEOUT 3000
GO

if not exists (select 1 from sys.tables where object_id = object_id('dba_util.drp_group') and schema_id = schema_id('dba_util'))
begin
/*
Notes                                    Time (must be a positive value)
level1 = frequent access (primary db)    1440 hrs    60 days
level2 = infrequent access (archive db)  4320 hrs    180 days
level3 = offline archive (S3 data)       8760 hrs    365 days    1 year
                                         17520 hrs   730 days    2 years
                                         21840 hrs   910 days    2.5 years
                                         1-9 years
mode = 0, Off
mode = 1, Move to next level
mode = 2, Delete
*/
    create table dba_util.drp_group (
        drp_group_id    int not null,
        group_name      varchar(100) not null,
        level1_mode     tinyint not null,
        level1_time     int not null,
        level2_mode     tinyint not null,
        level2_time     int not null,
        level3_mode     tinyint not null,
        level3_time     int not null
    );

    create unique index IX_drp_group on dba_util.drp_group (drp_group_id) include (group_name);
    create index IX_drp_group_level1 on dba_util.drp_group (level1_mode) include (level1_time);
    create index IX_drp_group_level2 on dba_util.drp_group (level2_mode) include (level2_time);
    create index IX_drp_group_level3 on dba_util.drp_group (level3_mode) include (level3_time);
end
go

if exists (select 1 from sys.tables where object_id = object_id('dba_util.drp_group') and schema_id = schema_id('dba_util'))
begin
    truncate table dba_util.drp_group;

    insert dba_util.drp_group (drp_group_id,group_name,level1_mode,level1_time,level2_mode,level2_time,level3_mode,level3_time)
        values (1, 'Entity metadata', 0, 1440, 0, 4320, 0, 8760);
    insert dba_util.drp_group (drp_group_id,group_name,level1_mode,level1_time,level2_mode,level2_time,level3_mode,level3_time)
        values (2, 'Summary log', 2, 2, 0, 4320, 0, 8760);
    insert dba_util.drp_group (drp_group_id,group_name,level1_mode,level1_time,level2_mode,level2_time,level3_mode,level3_time)
        values (3, 'Detail log', 2, 2, 0, 4320, 0, 8760);
    insert dba_util.drp_group (drp_group_id,group_name,level1_mode,level1_time,level2_mode,level2_time,level3_mode,level3_time)
        values (4, 'General log', 2, 2, 0, 4320, 0, 8760);
    insert dba_util.drp_group (drp_group_id,group_name,level1_mode,level1_time,level2_mode,level2_time,level3_mode,level3_time)
        values (5, 'Configuration', 0, 1440, 0, 4320, 0, 8760);
    insert dba_util.drp_group (drp_group_id,group_name,level1_mode,level1_time,level2_mode,level2_time,level3_mode,level3_time)
        values (6, 'Debug log', 0, 1440, 0, 4320, 0, 8760);
    insert dba_util.drp_group (drp_group_id,group_name,level1_mode,level1_time,level2_mode,level2_time,level3_mode,level3_time)
        values (7, 'Entity metadata archive', 0, 1440, 0, 4320, 0, 8760);
    insert dba_util.drp_group (drp_group_id,group_name,level1_mode,level1_time,level2_mode,level2_time,level3_mode,level3_time)
        values (8, 'Summary log archive', 0, 1440, 0, 4320, 0, 8760);
    insert dba_util.drp_group (drp_group_id,group_name,level1_mode,level1_time,level2_mode,level2_time,level3_mode,level3_time)
        values (9, 'Detail log archive', 0, 1440, 0, 4320, 0, 8760);
    insert dba_util.drp_group (drp_group_id,group_name,level1_mode,level1_time,level2_mode,level2_time,level3_mode,level3_time)
        values (10, 'General log archive', 0, 1440, 1, 4320, 0, 8760);
end
go

-- drop table dba_util.drp_table;
if not exists (select 1 from sys.tables where object_id = object_id('dba_util.drp_table') and schema_id = schema_id('dba_util'))
begin
    create table dba_util.drp_table (
        drp_table_id        int not null,
        drp_group_id        int not null,
        is_active           bit not null,
        table_name          varchar(100) not null,
        column_name_id      varchar(50) not null,
        column_name_date    varchar(50) not null,
        chunk_size          int not null,
        repository_provider varchar(50) not null,
        method              varchar(50) not null,
        command             varchar(100) not null,
        parameter_xml       nvarchar(max)
    );

    create unique index IX_drp_table on dba_util.drp_table (drp_table_id, drp_group_id, is_active) include (table_name);
end
go

if exists (select 1 from sys.tables where object_id = object_id('dba_util.drp_table') and schema_id = schema_id('dba_util'))
begin
    truncate table dba_util.drp_table;

    declare @xml nvarchar(max);
    set @xml = N'<parameter name="db_name"><value>@@db_name</value></parameter>' +
                '<parameter name="db_primary"><value>@@db_primary</value></parameter>' +
                '<parameter name="db_schema"><value>@@db_schema</value></parameter>' +
                '<parameter name="table_name"><value>@@table_name</value></parameter>' +
                '<parameter name="column_name_id"><value>@@column_name_id</value></parameter>' +
                '<parameter name="column_name_date"><value>@@column_name_date</value></parameter>' +
                '<parameter name="chunk_size"><value>@@chunk_size</value></parameter>' +
                '<parameter name="date_drp"><value>@@date_drp</value></parameter>';

    insert dba_util.drp_table (drp_table_id,drp_group_id,is_active,table_name,column_name_id,column_name_date,chunk_size,repository_provider,method,command,parameter_xml)
        values (200, 2, 1, 'log_summary', 'log_summary_id', 'request_date', 10000, 'sql', 'proc', 'dba_util.drp_generic_delete', @xml);

    insert dba_util.drp_table (drp_table_id,drp_group_id,is_active,table_name,column_name_id,column_name_date,chunk_size,repository_provider,method,command,parameter_xml)
        values (300, 3, 0, 'detail_log', 'detail_log_id', 'date_attempted', 10000, 'sql', 'proc', 'dba_util.drp_generic_delete', @xml);
    insert dba_util.drp_table (drp_table_id,drp_group_id,is_active,table_name,column_name_id,column_name_date,chunk_size,repository_provider,method,command,parameter_xml)
        values (301, 3, 0, 'event_log', '', 'request_date', 10000, 'aurora', '', '', '');
    insert dba_util.drp_table (drp_table_id,drp_group_id,is_active,table_name,column_name_id,column_name_date,chunk_size,repository_provider,method,command,parameter_xml)
        values (302, 3, 0, 'event_log', '', 'request_date', 10000, 'redshift', '', '', '');
    insert dba_util.drp_table (drp_table_id,drp_group_id,is_active,table_name,column_name_id,column_name_date,chunk_size,repository_provider,method,command,parameter_xml)
        values (303, 3, 0, 'event_log', '', 'request_date', 10000, 'athena', '', '', '');

    insert dba_util.drp_table (drp_table_id,drp_group_id,is_active,table_name,column_name_id,column_name_date,chunk_size,repository_provider,method,command,parameter_xml)
        values (400, 4, 0, 'general_log', 'general_log_id', 'date_sent', 10000, 'sql', 'proc', 'dba_util.drp_generic_delete', @xml);
    insert dba_util.drp_table (drp_table_id,drp_group_id,is_active,table_name,column_name_id,column_name_date,chunk_size,repository_provider,method,command,parameter_xml)
        values (401, 4, 1, 'history', 'history_id', 'history_date', 10000, 'sql', 'proc', 'dba_util.drp_generic_delete', @xml);
    insert dba_util.drp_table (drp_table_id,drp_group_id,is_active,table_name,column_name_id,column_name_date,chunk_size,repository_provider,method,command,parameter_xml)
        values (402, 4, 1, 'audit_log', 'audit_log_id', 'request_date', 10000, 'sql', 'proc', 'dba_util.drp_generic_delete', @xml);

    insert dba_util.drp_table (drp_table_id,drp_group_id,is_active,table_name,column_name_id,column_name_date,chunk_size,repository_provider,method,command,parameter_xml)
        values (1000, 10, 0, 'posting_log', 'posting_log_id', 'date_sent', 10000, 'sql', 'proc', 'dba_util.drp_generic_delete', @xml);
end
go

-- dba_util.drp_table_tree;
if not exists (select 1 from sys.tables where object_id = object_id('dba_util.drp_table_tree') and schema_id = schema_id('dba_util'))
begin
    create table dba_util.drp_table_tree (
        drp_table_id     int not null,
        order_id         int not null,
        is_active        bit not null,
        table_name       varchar(100) not null,
        column_name_id   varchar(50) not null
    );

    create unique index IX_drp_table_tree on dba_util.drp_table_tree (drp_table_id, order_id, is_active) include (table_name);
end
go

if exists (select 1 from sys.tables where object_id = object_id('dba_util.drp_table_tree') and schema_id = schema_id('dba_util'))
begin
    truncate table dba_util.drp_table_tree;

    insert dba_util.drp_table_tree (drp_table_id,order_id,is_active,table_name,column_name_id)
        values (200, 1, 1, 'log_summary', 'log_summary_id');
    insert dba_util.drp_table_tree (drp_table_id,order_id,is_active,table_name,column_name_id)
        values (200, 2, 1, 'log_summary_v2', 'log_summary_id');
    insert dba_util.drp_table_tree (drp_table_id,order_id,is_active,table_name,column_name_id)
        values (200, 3, 1, 'log_summary_v3', 'log_summary_id');
end
go

-- drp_status table
if exists (select 1 from sys.tables where object_id = object_id('dba_util.drp_status') and schema_id = schema_id('dba_util'))
    drop table dba_util.drp_status;
go

if not exists (select 1 from sys.tables where object_id = object_id('dba_util.drp_status') and schema_id = schema_id('dba_util'))
begin
    create table dba_util.drp_status (
        drp_table_id        int not null,
        drp_min_date        datetime not null,
        table_name          varchar(100) not null,
        column_name_id      varchar(50) not null,
        column_name_date    varchar(50) not null,
        repository_provider varchar(50) not null,
        date_updated        datetime not null,
        parameter_xml       varchar(max)
    );

    create unique index IX_drp_status on dba_util.drp_status (drp_table_id) include (drp_min_date);
end
go

-- drp_work_spid table
if exists (select 1 from sys.tables where object_id = object_id('dba_util.drp_work_spid') and schema_id = schema_id('dba_util'))
    drop table dba_util.drp_work_spid;
go

if not exists (select 1 from sys.tables where object_id = object_id('dba_util.drp_work_spid') and schema_id = schema_id('dba_util'))
begin
    create table dba_util.drp_work_spid (
        drp_work_spid   bigint not null,    -- @@spid of the session
        drp_step_id     int not null,        -- 0 = init, 1 = export, 2 = delete
        drp_min_date    datetime not null,
        drp_max_date    datetime not null,
        column_id_value bigint not null        -- target table id values
    );

    create clustered index IX_drp_work_spid on dba_util.drp_work_spid (drp_work_spid, drp_step_id, column_id_value);
end
go
