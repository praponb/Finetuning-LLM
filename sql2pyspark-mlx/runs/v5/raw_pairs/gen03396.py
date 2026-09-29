from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("days_since_opened_date", "id")
w2 = Window.partitionBy("region", "city").orderBy("days_since_opened_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("status", "region").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region").orderBy("days_since_opened_date", "id")
w6 = Window.partitionBy("city").orderBy("discount", "id")
w7 = Window.partitionBy("status", "city").orderBy(F.col("sqft_avg_6").desc(), "id")

cleaned = (
    df_stores
    .filter(F.col("region").eqNullSafe(F.lit("South")))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

metrics = (
    cleaned
    .filter(F.col("days_since_opened_date") != 34)
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "days_since_opened_date",
        F.row_number().over(w).alias("event_seq"),
        F.avg("sqft").over(w2).alias("sqft_avg_6"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount"), F.col("c.status"))
)

with_windows = (
    with_latest
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w_part))
    .withColumn("next_days_since_opened_date", F.lead("days_since_opened_date", 1).over(w5))
    .withColumn("next_days_since_opened_date_2", F.lead("days_since_opened_date", 1).over(w6))
    .withColumn("sqft_avg_6_bucket", F.ntile(3).over(w7))
)

totals = (
    with_windows
    .rollup(
        F.when(F.col("next_days_since_opened_date") >= 87, "HIGH").otherwise("LOW").alias("next_days_since_opened_date_tier"),
        F.when(F.col("days_since_opened_date") >= 66, "LARGE").otherwise("SMALL").alias("days_since_opened_date_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_rows"),
        F.percentile_approx("next_days_since_opened_date", [0.25, 0.5, 0.75]).alias("next_days_since_opened_date_quartiles"),
    )
)

result = (
    totals
    .select(
        "next_days_since_opened_date_tier",
        "days_since_opened_date_tier",
        "pending_rows",
        "next_days_since_opened_date_quartiles",
    )
    .orderBy(F.col("days_since_opened_date_tier").desc())
)
