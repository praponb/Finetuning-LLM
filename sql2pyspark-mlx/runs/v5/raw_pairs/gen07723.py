from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("carrier").orderBy("status_max_cost", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-08-03").cast("date")) & (F.year("delivered_date") >= 2025))
    .withColumn("delivered_date_period", F.date_trunc("MONTH", "delivered_date"))
    .withColumn("carrier_upper", F.upper("carrier"))
)

metrics = (
    cleaned
    .filter(F.col("delivered_date") > "2024-10-01")
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "carrier_upper",
        F.max("cost").over(w).alias("status_max_cost"),
        F.ntile(5).over(w2).alias("cost_bucket"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "shipped_date",
        "cost",
        F.min("status_max_cost").over(w3).alias("last3_min_status_max_cost"),
        F.count("status_max_cost").over(w3).alias("last3_count_status_max_cost"),
        F.max("status_max_cost").over(w3).alias("last3_max_status_max_cost"),
        F.sum("status_max_cost").over(w3).alias("last3_sum_status_max_cost"),
    )
)

filtered = (
    with_windows
    .filter(F.col("last3_min_status_max_cost").isNotNull())
    .select(
        "id",
        "last3_count_status_max_cost",
        "last3_max_status_max_cost",
        "last3_sum_status_max_cost",
        F.least("cost", "last3_count_status_max_cost").alias("least_cost_last3_count_status_max_cost"),
    )
)

result = (
    filtered
    .withColumn("least_cost_last3_count_status_max_cost_tier", F.when(F.col("least_cost_last3_count_status_max_cost") >= 72, "PREMIUM").otherwise("STANDARD"))
    .withColumn("last3_max_status_max_cost_tier", F.when(F.col("last3_max_status_max_cost") >= 83, "HIGH").otherwise("LOW"))
    .rollup(
        "least_cost_last3_count_status_max_cost_tier",
        "last3_max_status_max_cost_tier",
    )
    .agg(
        F.percentile_approx("least_cost_last3_count_status_max_cost", 0.5).alias("least_cost_last3_count_status_max_cost_p50"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("least_cost_last3_count_status_max_cost_tier").alias("gid"),
    )
    .orderBy("last3_max_status_max_cost_tier")
)
