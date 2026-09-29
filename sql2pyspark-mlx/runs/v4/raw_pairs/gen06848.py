from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy("account_type", "customer_id")
w3 = Window.partitionBy("week_start").orderBy(F.col("avg_row_seq").desc())

enriched = (
    df_accounts
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("account_type_avg_balance", F.avg("balance").over(w2))
)

result = (
    enriched
    .groupBy(
        F.date_trunc("week", "opened_date").alias("week_start"),
        F.date_trunc("month", "opened_date").alias("activity_month"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", F.col("account_type_avg_balance"))).alias("closed_total_account_type_avg_balance"),
        F.max("row_seq").alias("max_row_seq"),
        F.round(F.sum("account_type_avg_balance") / F.nullif(F.count(F.when(F.col("status") == "FROZEN", 1)), F.lit(0)), 2).alias("account_type_avg_balance_per_frozen"),
        F.percentile_approx("account_type_avg_balance", 0.5).alias("account_type_avg_balance_p50"),
        F.round(F.sum("account_type_avg_balance") / F.nullif(F.count(F.when(F.col("status") == "OPEN", 1)), F.lit(0)), 2).alias("account_type_avg_balance_per_open"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
    )
    .filter(F.col("account_type_avg_balance_per_open") < 0)
    .withColumn("avg_row_seq_rank", F.rank().over(w3))
)
