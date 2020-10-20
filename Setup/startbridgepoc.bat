@echo off
Rem REQUIREMENTS
Rem Docker and docker-compose cmd line tools for windows, and docker desktop for windows
Rem SQL Server 2016 with SQL Server Agent Running
Rem CDC Enabled
Rem Debezium Login Created for MASTER db
Rem Debezium User Created for CIMS with DB_owner set (There is a file for this)
Rem Debezium files have been moved to root of d:\ drive and path has been set in docker-compose file (line 61)
Rem Set database.hostname IP address in claims.json to your local IP (This is so the docker container knows how to communicate with the host machine running SQL server)
Rem In conductor create new topic using the name "CIMSTEST.Test"
Rem In conductor create new schema using claimBlackListValue.avsc and call it CIMTSTEST.Test-value
Rem BridgePOC is running MainApp in Intellij

pushd %~dp0
echo Moving Debezium files to D:\debezium-connector-sqlserver
echo D|xcopy /E ..\debezium-connector-sqlserver D:\debezium-connector-sqlserver
echo Starting SQL Server Agent:
net start "SQL Server Agent (MSSQLSERVER)"
echo Running SQL Setup
sqlcmd -S %COMPUTERNAME% -i .\setup.sql
echo Starting Docker Containers
docker-compose up -d
echo WAITING 30 SECONDS
timeout /t 30
echo Setting up Connectors
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @connector-setup.json