from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id")
w2 = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("plan", "customer_id").orderBy(F.col("end_date").desc_nulls_last(), "id")

metrics = (
    df_subscriptions
    .select(
        "*",
        F.lag("monthly_fee", 2).over(w).alias("prev_monthly_fee"),
        F.first("monthly_fee", ignorenulls=True).over(w2).alias("first_known_monthly_fee"),
        F.avg("monthly_fee").over(w3).alias("plan_avg_monthly_fee"),
    )
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

attributed = (
    ranked
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.start_date") >= F.col("sub.start_date")) & (F.col("src.start_date") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

result = (
    attributed
    .groupBy("subscription_id")
    .agg(
        F.percentile_approx("prev_monthly_fee", [0.25, 0.5, 0.75]).alias("prev_monthly_fee_quartiles"),
        F.percentile_approx("subscription_id", [0.25, 0.5, 0.75]).alias("subscription_id_quartiles"),
    )
)
