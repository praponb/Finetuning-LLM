from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("plan", "status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")

with_windows = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 80)
    .select(
        "*",
        F.lag("monthly_fee", 1).over(w).alias("prev_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("cumulative_monthly_fee"),
        F.first("monthly_fee", ignorenulls=True).over(w3).alias("first_known_monthly_fee"),
        F.lag("monthly_fee", 1).over(w4).alias("prev_monthly_fee_2"),
    )
)

result = (
    with_windows
    .withColumn("prev_monthly_fee_tier", F.when(F.col("prev_monthly_fee") >= 213, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["status", "customer_id", "prev_monthly_fee_tier"], ["status", "customer_id"]],
        "status",
        "customer_id",
        "prev_monthly_fee_tier",
    )
    .agg(
        F.percentile_approx("cumulative_monthly_fee", [0.25, 0.5, 0.75]).alias("cumulative_monthly_fee_quartiles"),
        F.percentile_approx("monthly_fee", 0.9).alias("monthly_fee_p90"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
    .orderBy(F.col("num_plans").desc_nulls_last())
)
