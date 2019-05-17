# QuerySQLtoAWSGlue
Built in go 1.12.2. 

Need yaml file which format like this

databases:
  db1:
    dbtype: mysql
    dbuser: root
    dbpassword: root
    dbip: 
    dbport: 
    dbName: 

storages:
  st1:    
    s3bucket: 
    s3region: 

exporters:
  - scheduler: '30 * * * * *'
    query: 
    querykey: 
    database: db1
    storage: st1
    localfolder: 
    subfolderinaws: 
    filename: 
        
