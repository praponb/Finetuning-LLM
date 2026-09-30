from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id")
w2 = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("days_since_start_date").desc())
w4 = Window.partitionBy("customer_id", "status").orderBy("days_since_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-09-04").cast("date")) & (F.col("status") == "PAUSED"))
    .select("*", F.upper("status").alias("status_upper"))
)

base = (
    cleaned
    .filter((F.col("end_date") > F.to_date(F.lit("2023-11-24"))) & F.col("customer_id").isNotNull())
    .withColumn("status_upper_2", F.upper("status"))
    .withColumn("days_since_start_date", F.datediff(F.current_date(), "start_date"))
)

base_subscriptions = (
    base
    .filter(F.col("end_date") >= F.lit("2024-02-03").cast("date"))
    .select("*", F.initcap(F.trim("status")).alias("status_title"))
)

scored = (
    base_subscriptions
    .select(
        "*",
        F.lag("monthly_fee", 1).over(w).alias("prev_monthly_fee"),
        F.sum("days_since_start_date").over(w2).alias("cumulative_days_since_start_date"),
        F.rank().over(w3).alias("days_since_start_date_rank"),
        F.last("monthly_fee", ignorenulls=True).over(w4).alias("last_known_monthly_fee"),
        F.first("days_since_start_date", ignorenulls=True).over(w5).alias("first_known_days_since_start_date"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("days_since_start_date") >= 86, "LARGE").otherwise("SMALL").alias("days_since_start_date_tier"),
        "customer_id",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("first_known_days_since_start_date", 0.95).alias("first_known_days_since_start_date_p95"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("distinct_ids") < 1)
)
