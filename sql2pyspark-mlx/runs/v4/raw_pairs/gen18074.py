from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_recent = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant")

scored = (
    df_transactions
    .select(
        "*",
        F.avg("amount").over(w).alias("amount_avg_90d"),
        F.max("amount").over(w_recent).alias("amount_max_7d"),
        F.avg("amount").over(w3).alias("amount_avg_5"),
        F.first("merchant", ignorenulls=True).over(w4).alias("first_known_merchant"),
        F.count("amount").over(w5).alias("merchant_count_amount"),
    )
)

result = (
    scored
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("activity_month"))
    .agg(
        F.percentile_approx("amount_max_7d", [0.25, 0.5, 0.75]).alias("amount_max_7d_quartiles"),
        F.count_if(F.col("merchant") == "Lazada").alias("lazada_events"),
        F.count_if(F.col("txn_type") == "CREDIT").alias("credit_events"),
    )
    .filter(F.col("lazada_events") > 1)
    .orderBy(F.col("lazada_events").desc_nulls_last())
)
