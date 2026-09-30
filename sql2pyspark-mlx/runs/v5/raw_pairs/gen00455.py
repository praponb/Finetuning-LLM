from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan", "customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        F.last("monthly_fee", ignorenulls=True).over(w).alias("last_known_monthly_fee"),
        F.last("monthly_fee", ignorenulls=True).over(w2).alias("last_known_monthly_fee_2"),
        F.last("monthly_fee", ignorenulls=True).over(w3).alias("last_known_monthly_fee_3"),
    )
)

result = (
    scored
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("last_known_monthly_fee_2", 0.9).alias("last_known_monthly_fee_2_p90"),
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_count"),
        F.round(F.avg("last_known_monthly_fee"), 2).alias("avg_last_known_monthly_fee"),
    )
    .filter(F.col("event_count") > 1)
    .orderBy(F.col("last_known_monthly_fee_2_p90").desc())
)
