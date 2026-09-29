from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc(), "id")
w_part = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city", "status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(140, 200))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.upper("plan").alias("plan_upper"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select("s.*", F.col("c.city"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.last("city", ignorenulls=True).over(w_part).alias("last_known_city"),
        F.last("city", ignorenulls=True).over(w3).alias("last_known_city_2"),
    )
)

base = (
    with_windows
    .filter(F.col("city").isNotNull() & F.col("city").isin(["Berlin", "Chiang Mai"]))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "plan_upper",
        "row_seq",
        "last_known_city",
        F.date_trunc("WEEK", "end_date").alias("end_date_period"),
        F.upper("city").alias("city_upper"),
    )
)

filtered = (
    base
    .filter((F.col("start_date") >= F.lit("2023-04-13").cast("date")) & F.col("status").isin(["EXPIRED", "CANCELLED", "PAUSED"]))
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "row_seq",
        "last_known_city",
        "city_upper",
        F.col("row_seq").cast("double").alias("row_seq_num"),
    )
)

result = (
    filtered
    .rollup(
        F.when(F.col("row_seq") >= 8, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
        "status",
    )
    .agg(
        F.avg(F.when(F.col("status") == "PAUSED", F.col("row_seq"))).alias("paused_avg_row_seq"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.percentile_approx("row_seq_num", 0.95).alias("row_seq_num_p95"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.min("row_seq").alias("min_row_seq"),
    )
)
