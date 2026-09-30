from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("monthly_fee", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("last4_sum_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("last4_count_monthly_fee").desc(), "id")
w4 = Window.partitionBy("country").orderBy("last4_avg_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country", "plan").orderBy("last4_max_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("plan").orderBy(F.col("running_max_cumulative_last4_sum_monthly_fee").desc(), "id")

with_dims = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.city").isNull())
    .select(
        "a.id",
        "a.customer_id",
        "a.plan",
        "a.start_date",
        "a.monthly_fee",
        "a.status",
        F.col("p.city"),
        F.col("p.name"),
        F.col("p.country"),
    )
)

metrics = (
    with_dims
    .select(
        "id",
        "plan",
        "monthly_fee",
        "city",
        "name",
        "country",
        F.count("monthly_fee").over(w).alias("last4_count_monthly_fee"),
        F.avg("monthly_fee").over(w).alias("last4_avg_monthly_fee"),
        F.sum("monthly_fee").over(w).alias("last4_sum_monthly_fee"),
        F.max("monthly_fee").over(w).alias("last4_max_monthly_fee"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "plan",
        "monthly_fee",
        "city",
        "country",
        "last4_avg_monthly_fee",
        "last4_max_monthly_fee",
        F.last("last4_avg_monthly_fee", ignorenulls=True).over(w2).alias("last_known_last4_avg_monthly_fee"),
        F.ntile(4).over(w3).alias("last4_count_monthly_fee_bucket"),
        F.sum("last4_sum_monthly_fee").over(w4).alias("cumulative_last4_sum_monthly_fee"),
    )
)

metrics_subscriptions = (
    with_windows
    .select(
        "id",
        "plan",
        "last4_max_monthly_fee",
        "last_known_last4_avg_monthly_fee",
        F.avg("cumulative_last4_sum_monthly_fee").over(w5).alias("running_avg_cumulative_last4_sum_monthly_fee"),
        F.max("cumulative_last4_sum_monthly_fee").over(w5).alias("running_max_cumulative_last4_sum_monthly_fee"),
        F.min("cumulative_last4_sum_monthly_fee").over(w5).alias("running_min_cumulative_last4_sum_monthly_fee"),
    )
)

ranked = (
    metrics_subscriptions
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select(
        "id",
        "plan",
        "last4_max_monthly_fee",
        "last_known_last4_avg_monthly_fee",
        "running_avg_cumulative_last4_sum_monthly_fee",
        "running_max_cumulative_last4_sum_monthly_fee",
        "row_num",
    )
)
