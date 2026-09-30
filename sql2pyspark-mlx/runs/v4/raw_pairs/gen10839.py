from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") > "2025-02-01")
    .groupBy("status")
    .pivot("account_type", ["CREDIT", "CHECKING"])
    .agg(F.sum("balance"))
    .orderBy("status")
)
