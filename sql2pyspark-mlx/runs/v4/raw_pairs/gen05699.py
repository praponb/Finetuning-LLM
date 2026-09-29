from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy(F.col("opened_date").desc_nulls_first(), "id")

ranked = (
    df_accounts
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy(F.date_trunc("quarter", "opened_date").alias("quarter_start"))
    .agg(
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "CREDIT", 1)), F.lit(0)), 2).alias("balance_per_credit"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_rows"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
    )
    .orderBy(F.col("balance_per_credit").asc_nulls_last())
)
