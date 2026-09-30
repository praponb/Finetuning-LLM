from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc_nulls_first(), "id")
w2 = Window.partitionBy("week_start").orderBy(F.col("loan_count").desc())

deduped = (
    df_accounts
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .groupBy(F.date_trunc("week", "opened_date").alias("week_start"), "customer_id")
    .agg(
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_rows"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "FROZEN", 1)), F.lit(0)), 2).alias("balance_per_frozen"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
    )
    .withColumn("loan_count_percent_rank", F.percent_rank().over(w2))
    .orderBy(F.col("balance_per_frozen").desc_nulls_first())
)
