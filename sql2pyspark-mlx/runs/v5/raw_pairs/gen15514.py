from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") > "2023-05-17")
    .groupBy("status")
    .pivot("account_type", ["SAVINGS", "CREDIT", "LOAN", "CHECKING"])
    .agg(F.max("balance"))
    .orderBy("status")
)
