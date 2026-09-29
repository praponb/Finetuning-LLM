from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee").desc_nulls_last())
w2 = Window.partitionBy("plan", "status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_subscriptions
    .withColumn("monthly_fee_dense_rank", F.dense_rank().over(w))
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w2))
)

result = (
    scored
    .withColumn("monthly_fee_dense_rank_tier", F.when(F.col("monthly_fee_dense_rank") >= 9, "HIGH").otherwise("LOW"))
    .withColumn("week_start", F.date_trunc("WEEK", "end_date"))
    .groupingSets(
        [["monthly_fee_dense_rank_tier", "week_start"], ["monthly_fee_dense_rank_tier"], []],
        "monthly_fee_dense_rank_tier",
        "week_start",
    )
    .agg(
        F.sum("cumulative_monthly_fee").alias("total_cumulative_monthly_fee"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.min("cumulative_monthly_fee").alias("min_cumulative_monthly_fee"),
        F.count("*").alias("cnt"),
        F.percentile_approx("monthly_fee_dense_rank", [0.25, 0.5, 0.75]).alias("monthly_fee_dense_rank_quartiles"),
    )
)
