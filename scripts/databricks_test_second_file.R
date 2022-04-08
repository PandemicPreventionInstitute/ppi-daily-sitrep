# Test of subsequent R script using a generated dataset

if (USE_CASE == 'databricks'){
    test <-read.csv("/dbfs/FileStore/tables/ppi-daily-sitrep/data/test_db.csv")
}

print(test)
