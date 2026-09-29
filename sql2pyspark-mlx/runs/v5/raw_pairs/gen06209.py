from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_last())
w2 = Window.partitionBy("customer_id", "plan").orderBy("end_date", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")

scored = (
    df_subscriptions
    .filter(F.col("start_date") <= "2025-04-01")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.rank().over(w).alias("monthly_fee_rank"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_5"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_3_prior"),
        F.ntile(5).over(w4).alias("monthly_fee_bucket"),
    )
)

result = (
    scored
    .groupBy("status")
    .agg(
        F.sum("monthly_fee_avg_5").alias("total_monthly_fee_avg_5"),
        F.round(F.avg("monthly_fee_avg_5"), 2).alias("avg_monthly_fee_avg_5"),
        F.percentile_approx("monthly_fee_rank", [0.25, 0.5, 0.75]).alias("monthly_fee_rank_quartiles"),
    )
    .filter(F.col("avg_monthly_fee_avg_5") < 1000000)
)
