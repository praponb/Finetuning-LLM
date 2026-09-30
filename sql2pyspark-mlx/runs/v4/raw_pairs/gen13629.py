from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc())

scored = (
    df_accounts
    .withColumn("balance_bucket", F.ntile(4).over(w))
    .withColumn("balance_dense_rank", F.dense_rank().over(w2))
)

result = (
    scored
    .groupBy(
        F.when(F.col("balance") >= 62000, "HIGH").otherwise("LOW").alias("balance_tier"),
    )
    .agg(
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"),
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance_bucket")).otherwise(0)).alias("credit_balance_bucket"),
        F.round(F.sum("balance_bucket") / F.nullif(F.count(F.when(F.col("account_type") == "CREDIT", 1)), F.lit(0)), 2).alias("balance_bucket_per_credit"),
        F.sum(F.when(F.col("status") == "OPEN", F.col("balance_dense_rank")).otherwise(0)).alias("open_balance_dense_rank"),
    )
    .filter(F.col("open_balance_dense_rank") > 0)
)
