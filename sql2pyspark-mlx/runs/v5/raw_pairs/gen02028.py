from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .pivot("account_type", ["LOAN", "CHECKING"])
    .agg(F.max("balance").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
)
