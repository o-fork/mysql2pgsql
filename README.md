mysql2pgsql
===========

Tool for migrating a full database from mysql to postgresql

#Build
```sh
mvn clean install 
```

#Run
```sh
java -Xmx4G -jar target/mysql2pgsql-1.0-SNAPSHOT.jar mysqlhost mysqlport mysqluser pgsqlhost pgsqldb pgsqlport pgsqluser schema
```
The heap size may need to be modified to match the largest table. Mysql's jdbc driver doesn't seem to support limited fetch sizes so all rows are read before writing to postgres

#Flow of operation
1. Dump mysql schema using mysqldump
2. Convert schema to pgsql format
3. Create schema and tables in pgsql with converted schema file. No indexes or constraints applied in this phase.
4. Migrate all data from mysql to pgsql using jdbc
5. Create primary keys
6. Create indexes and other constraints
7. Update all sequences to the current max value of each serial column

#Not converted by this tool
* Views
* Functions
* Users/Accounts
* Table partitioning


#Misc
Initial attempt was made with bash and python, using mysqldump to transport all data. This solution was quite slow, and when tested, jdbc and batching inserts turned out to be four times faster.
The data types converted by this utility so far is based on a real life mysql instance: there is probably quite a few types that are missing. To be improved.


