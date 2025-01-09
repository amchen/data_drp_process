set ansi_padding, ansi_warnings, concat_null_yields_null, arithabort, quoted_identifier, ansi_nulls on;
go
set lock_timeout 3000;
go
if object_id('dba_util.drp_generic_copy_s3','P') is not null
begin
    set noexec on;
end
go
create procedure dba_util.drp_generic_copy_s3 as select 1 as one
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
insert config_key_value_pairs (cake_key,value_string) values ('drp_bucket_primary','data-drp-test-us1');
insert config_key_value_pairs (cake_key,value_string) values ('drp_bucket_secondary','data-drp-test-us2');
*/
alter procedure [dba_util].[drp_generic_copy_s3]
    @sqls3 varchar(max) = null,
    @dt_path varchar(100) = null,
    @table_name varchar(100) = null,
    @debug_only bit = 1
as
begin        
    set ansi_padding, ansi_warnings, concat_null_yields_null, arithabort, quoted_identifier, ansi_nulls, nocount, xact_abort on;
    set nocount on;

    declare @drp_kvp varchar(20) = 'drp_bucket_';
    declare @msg varchar(max);
    declare @proc_name varchar(255) = object_name(@@procid);

    -- sanity check
    if (@sqls3 is null or @dt_path is null or @table_name is null)
    begin
        set @msg = '@sqls3 cannot be NULL';
        exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
        raiserror(@msg,16,1);
        return;
    end

    declare @driveOutput varchar(3);
    declare @driveBin varchar(3);

    -- get drive letters
    select @driveOutput = config_value from dba.dbo.config where config_name = 'drive.sqlS3.output';
    set @driveOutput = ISNULL(@driveOutput, 'd:');
    select @driveBin = config_value from dba.dbo.config where config_name = 'drive.sqlS3.bin';
    set @driveBin = ISNULL(@driveBin, 'd:');

    declare @instance_id int = (select top 1 instance_id from global_settings order by instance_id);

    declare @aws_region_primary varchar(20);
    declare @s3_bucket_primary varchar(200);
    declare @s3_bucket_primary_enable bit;
    declare @aws_region_secondary varchar(20);
    declare @s3_bucket_secondary varchar(200);
    declare @s3_bucket_secondary_enable bit;

    set @aws_region_primary = dbo.ckvp_string('aws_region_primary');
    set @s3_bucket_primary = dbo.ckvp_string(@drp_kvp + 'primary');
    set @s3_bucket_primary_enable = isnull(dbo.ckvp_bool(@drp_kvp + 'primary'), 1);
    set @aws_region_secondary = dbo.ckvp_string('aws_region_secondary');
    set @s3_bucket_secondary = dbo.ckvp_string(@drp_kvp + 'secondary');
    set @s3_bucket_secondary_enable = isnull(dbo.ckvp_bool(@drp_kvp + 'secondary'), 1);

    declare @allow_empty bit = 1;        -- don't throw an error if there aren't any rows
    declare @sql nvarchar(max);
    declare @sql_parms nvarchar(100);
    declare @filetarget varchar(200);
    declare @filename varchar(300);
    declare @aws_file_key varchar(300);

    declare @bcp_cmd varchar(8000);
    declare @powershell_cmd varchar(8000);
    declare @bcp_sql varchar(1000);
    declare @file_status int;
    declare @import_errors table(cnt int);
    declare @awsaccesskey varchar(25) = dbo.ckvp_string('AWSAccessKey');
    declare @awssecret varchar(45)  = dbo.ckvp_string('AWSSecret');
    declare @dynamic_columns table (column_name nvarchar(max));
    declare @columns_to_remove table (column_name nvarchar(max));
    declare @columns_to_remove_text nvarchar(800);

    if @awsaccesskey is null
    begin
        set @msg = '@AWSAccessKey is not set.';
        exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
        raiserror(@msg,16,1);
        return 1;
    end

    if @s3_bucket_primary is null
    begin
        set @msg = '@s3_bucket_primary is not set for kvp = ' + @drp_kvp + 'primary';
        exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
        raiserror(@msg,16,1);
        return 1;
    end

    if @aws_region_primary is null or @aws_region_primary not in ('us-east-1','us-west-2')
    begin
        set @msg = 'Invalid AWS region: ' + isnull(@aws_region_primary, 'NULL');
        exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
        raiserror(@msg,16,1);
        return 1;
    end

    declare @now datetime = dbo.GetInstanceDate(NULL);
    declare @ts varchar(20);
    select @ts = convert(varchar, @now, 112) + replace(convert(varchar, @now, 108), ':', '');

    begin try
        set @filetarget = CAST(@instance_id AS VARCHAR) + '/' + @table_name + '/' + @dt_path;
        set @aws_file_key = @filetarget + '/' + @ts + '.csv.gz';
        set @filename = @driveOutput + '\sql\drp\' + CAST(@instance_id AS VARCHAR) + '\' + @table_name + '\' + @dt_path + '\' + @ts + '.csv';
        set @bcp_sql = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;SET QUOTED_IDENTIFIER, ANSI_NULLS ON;' + @sqls3;

        -- Extract Data
        set @bcp_cmd =  @driveBin + '\sql\tools\bin\sqlS3.exe'
            +' -keyName="'+@filetarget+'"'
            +' -sourceServer="'+@@SERVERNAME+'"'
            +' -sourceDatabase="'+DB_NAME()+'"'
            +' -query="'+@bcp_sql+'"'
            +' -targetFilename="'+@filename+'.gz"'
            +' -regionName="'+@aws_region_primary+'"'
            +' -bucketName="'+@s3_bucket_primary+'"'
            +' -AWSAccessKey="'+@AWSAccessKey+'"'
            +' -AWSSecret="'+@AWSSecret+'"'
            +' -timeoutSeconds=1800'
            +' -format="UTF8"'
            +' -allowEmptyFile="false"'
            +' -UseAcceleratedEndpoint="true"'
            ;

        exec dba.dba_util.print_log @log_msg = @bcp_cmd, @procedure_name = @proc_name;
            
        declare @results varchar(max) = '';
        if (@debug_only = 0)
        begin
            -- execute sqlS3.exe
            exec dba_util.cmdshell @cmd = @bcp_cmd, @showresults = 0, @results = @results output;

            if (@results like '%Error%') or (@results like '%Unhandled Exception%') or (@allow_empty = 0 and @results like '%Nothing to export%')
            begin
                exec dba.dba_util.print_log @log_msg = @results, @procedure_name = @proc_name;
                raiserror(@results,16,2);
            end

            -- cleanup folder
            set @bcp_cmd = 'rmdir /s /q "' + @driveOutput + '\sql\drp\' + CAST(@instance_id AS VARCHAR) + '\' + @table_name + '\"';
            exec dba.dba_util.print_log @log_msg = @bcp_cmd, @procedure_name = @proc_name;
            exec dba_util.cmdshell @cmd = @bcp_cmd, @showresults = 0, @results = @results output;
        end

        if (@s3_bucket_secondary is not null and @s3_bucket_secondary_enable = 1) 
        begin
            set @powershell_cmd = 'powershell Copy-S3Object -AccessKey "'+@AWSAccessKey+
                '" -SecretKey "'+@AWSSecret+
                '" -SourceRegion "'+@aws_region_primary+
                '" -BucketName "'+@s3_bucket_primary+
                '" -Key "'+@aws_file_key+
                '" -Region "'+@aws_region_secondary+
                '" -DestinationBucket "'+@s3_bucket_secondary+
                '" -DestinationKey "'+@aws_file_key+'"'
                ;

            exec dba.dba_util.print_log @log_msg = @powershell_cmd, @procedure_name = @proc_name;

            if (@debug_only = 0)
            begin
                set @results = ''
                exec dba_util.cmdshell @cmd = @powershell_cmd, @showresults = 0, @results = @results output;

                if (@results like '%Error%') or (@results like '%Unhandled Exception%') or (charindex('ETag',@results) = 0)
                begin
                    exec dba.dba_util.print_log @log_msg = @results, @procedure_name = @proc_name;
                    raiserror(@results,16,2);
                end
            end
        end
        else
        begin
            set @msg = 'Skip Copy-S3Object: s3_bucket_secondary is NULL or disabled';
            exec dba.dba_util.print_log @log_msg = @msg, @procedure_name = @proc_name;
        end
    end try
    begin catch
        if @@trancount > 0
            rollback;

        declare @subj varchar(250);
        declare @body varchar(max);
        declare @errormessage varchar(4000);
        declare @crlf char(2) = char(13) + char(10);

        select @subj = @@servername+ '-' + db_name() + ': ' + @proc_name + ' at ' + convert(varchar,getutcdate(),100) + ' UTC';

        SET @errorMessage = 'Error: '+  ERROR_MESSAGE();

        SET @body = @errorMessage
            + @crlf + @crlf + 'User: ' + user_name()
            + @crlf + @crlf + 'Parameters: '
            + @crlf + 'dbo.' + @proc_name + ' @print = 1'
            + @crlf + '--@filename = ' + coalesce(@filename,'NULL');
/*
        exec dbo.send_dbmail 
            @subject = @subj
            ,@recipients = 'amchen@yahoo.com'
            ,@body = @body;
*/
        exec dba.dba_util.print_log @log_msg = @errormessage, @procedure_name = @proc_name;
        raiserror(@errormessage, 15, 1);
        return 1;
    end catch

end
go
