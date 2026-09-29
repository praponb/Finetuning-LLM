from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("units", "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")
w3 = Window.orderBy(F.col("prev_revenue").asc_nulls_last())
w4 = Window.orderBy("prev_revenue", "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("prev_prev_units").desc_nulls_last(), "id")
w6 = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id")
w7 = Window.orderBy("prev_revenue_cume_dist", "id")

metrics = (
    df_store_sales
    .select(
        "*",
        F.lag("revenue").over(w).alias("prev_revenue"),
        F.lag("units").over(w).alias("prev_units"),
        F.row_number().over(w).alias("event_seq"),
    )
)

scored = (
    metrics
    .filter(F.col("revenue").isNull())
    .withColumn("prev_prev_units", F.lag("prev_units", 1).over(w2))
    .withColumn("prev_revenue_cume_dist", F.cume_dist().over(w3))
    .withColumn("prev_revenue_bucket", F.ntile(10).over(w4))
)

latest = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "prev_revenue",
        "prev_units",
        "event_seq",
        "prev_prev_units",
        "prev_revenue_cume_dist",
        "prev_revenue_bucket",
    )
)

with_windows = (
    latest
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        "prev_revenue_cume_dist",
        F.lead("prev_units", 1).over(w6).alias("next_prev_units"),
        F.lag("prev_units", 1).over(w7).alias("prev_prev_units_2"),
    )
)

grouped = (
    with_windows
    .groupBy(
        F.when(F.col("units") >= 32, "PREMIUM").otherwise("STANDARD").alias("units_tier"),
    )
    .agg(
        F.percentile_approx("next_prev_units", [0.25, 0.5, 0.75]).alias("next_prev_units_quartiles"),
        F.sum("prev_prev_units_2").alias("total_prev_prev_units_2"),
    )
)

result = (
    grouped
    .select("units_tier", "next_prev_units_quartiles")
    .orderBy(F.col("units_tier").desc())
    .limit(20)
)
