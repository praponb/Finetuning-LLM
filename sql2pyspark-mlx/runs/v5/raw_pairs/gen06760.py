from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_subscriptions
    .filter(F.col("plan") == "standard")
    .withColumn("monthly_fee_whole", F.floor("monthly_fee"))
    .withColumn("status_title", F.initcap(F.trim("status")))
)

with_windows = (
    filtered
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "status",
        "status_title",
        F.avg("monthly_fee").over(w).alias("last3_avg_monthly_fee"),
        F.max("monthly_fee").over(w).alias("last3_max_monthly_fee"),
        F.sum("monthly_fee").over(w).alias("last3_sum_monthly_fee"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("customer_id").isNotNull())
    .withColumn("last3_sum_monthly_fee_whole", F.floor("last3_sum_monthly_fee"))
)

remaining_keys = (
    cleaned
    .select("customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("status").isin(["EXPIRED", "ACTIVE", "CANCELLED"]))
        .select("customer_id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.country").alias("country"), F.col("p.city").alias("city"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("e.last3_avg_monthly_fee")), 2).alias("total_last3_avg_monthly_fee"),
        F.min(F.col("e.last3_sum_monthly_fee_whole")).alias("min_last3_sum_monthly_fee_whole"),
    )
    .orderBy(F.col("country").asc_nulls_last())
)
