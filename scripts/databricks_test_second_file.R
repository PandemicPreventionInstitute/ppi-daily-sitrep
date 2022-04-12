# Test of subsequent R script using a generated dataset

if (USE_CASE == 'databricks'){
    test <-read.csv("/dbfs/FileStore/tables/test_db.csv")
}

print(test)
