USE [master]
GO
CREATE LOGIN [debezium] WITH PASSWORD=N'Debetest01', DEFAULT_DATABASE=[master], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
GO
USE [CIMS]
GO
CREATE USER [debezium] FOR LOGIN [debezium]
GO
USE [CIMS]
GO
ALTER ROLE [db_owner] ADD MEMBER [debezium]
GO

-- Enable CDC on DB --
USE CIMS
GO
EXEC sys.sp_cdc_enable_db
GO

-- Enable CDC per table --
USE CIMS
GO

EXEC sys.sp_cdc_enable_table        
@source_schema = N'Financial',
@source_name   = N'Claim',
@role_name     = N'debezium'--,
GO

EXEC sys.sp_cdc_enable_table        
@source_schema = N'Financial',
@source_name   = N'ClaimStatus',
@role_name     = N'debezium'--,
GO

EXEC sys.sp_cdc_enable_table        
@source_schema = N'Financial',
@source_name   = N'ClaimStatusClaimLink',
@role_name     = N'debezium'--,
GO

EXEC sys.sp_cdc_enable_table        
@source_schema = N'Reference',
@source_name   = N'ClaimState',
@role_name     = N'debezium'--,
GO

-- Display affected tables in table view --
EXEC sys.sp_cdc_help_change_data_capture
GO