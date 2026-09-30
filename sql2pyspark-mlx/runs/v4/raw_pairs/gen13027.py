from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue_per_unit").desc_nulls_last(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("stock_qty").desc(), "id")
w_part = Window.partitionBy("product_id").orderBy(F.col("revenue").asc_nulls_first())
w_ordered = Window.partitionBy("product_id").orderBy(F.col("launch_date_dayofweek").desc(), "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-04-16").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        (F.col("units") * F.col("revenue")).alias("revenue_per_unit"),
    )
)

deduped = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_dims = (
    deduped
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.launch_date"), F.col("p.stock_qty"))
)

latest = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

valid = (
    latest
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        "revenue_per_unit",
        "stock_qty",
        "latest_rank",
        (F.col("units") / F.nullif("revenue_per_unit", F.lit(0))).alias("units_per_revenue_per_unit"),
        F.dayofweek("launch_date").alias("launch_date_dayofweek"),
    )
)

with_windows = (
    valid
    .withColumn("revenue_percent_rank", F.percent_rank().over(w_part))
    .withColumn("launch_date_dayofweek_bucket", F.ntile(4).over(w_ordered))
)

enriched = (
    with_windows
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.cost"))
)

result = (
    enriched
    .withColumn("revenue_percent_rank_tier", F.when(F.col("revenue_percent_rank") >= 0.61, "PREMIUM").otherwise("STANDARD"))
    .withColumn("units_per_revenue_per_unit_tier", F.when(F.col("units_per_revenue_per_unit") >= 73, "PREMIUM").otherwise("STANDARD"))
    .cube("product_id", "revenue_percent_rank_tier", "units_per_revenue_per_unit_tier")
    .agg(
        F.sum("units").alias("total_units"),
        F.percentile_approx("launch_date_dayofweek_bucket", [0.25, 0.5, 0.75]).alias("launch_date_dayofweek_bucket_quartiles"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("product_id").desc())
)
