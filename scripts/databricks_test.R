# test code for databricks
#USE_CASE <-'databricks'

if (USE_CASE =='domino'){
    secrets <- read.csv("/mnt/data/secrets_gisaid.csv", header = FALSE) #a file with the username on the first row and password on the second row. No header
}
if (USE_CASE == 'databricks'){
    secrets <-read.csv("/dbfs/FileStore/tables/ppi-daily-sitrep/data/secrets_gisaid.csv")
}
if (USE_CASE =='local'){
    secrets <- read.csv("../data/secrets_gisaid.csv", header = FALSE) #a file with the username on the first row and password on the second row. No header
}
user <- as.character(secrets[1,1])
pw <- as.character(secrets[2,1])

# Make a new csv that calls the secrets gisaid
user_pw_df<-data.frame(user, pw)
print(user_pw_df)

if (USE_CASE == 'databricks'){
    write.csv(user_pw_df, '/dbfs/FileStore/tables/test_db.csv', row.names = FALSE)
}

