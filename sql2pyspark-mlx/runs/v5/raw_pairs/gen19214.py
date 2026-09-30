from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("brand").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("brand").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy(F.col("running_sum_revenue").desc_nulls_first())

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-04-04").cast("date"))
    .withColumn("revenue_per_unit", (F.col("revenue") * F.col("units")))
    .withColumn("revenue_band", F.when(F.col("revenue") >= 4400, "High").when(F.col("revenue") >= 800, "Medium").otherwise("Low"))
)

with_dims = (
    valid
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .filter(F.col("d.supplier_id") == 23)
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.sale_date",
        "src.units",
        "src.revenue",
        "src.revenue_band",
        F.col("d.brand"),
    )
)

scored = (
    with_dims
    .withColumn("running_max_revenue", F.max("revenue").over(w_ordered))
    .withColumn("running_sum_revenue", F.sum("revenue").over(w_ordered))
    .withColumn("running_count_revenue", F.count("revenue").over(w_ordered))
    .withColumn("cumulative_revenue", F.sum("revenue").over(w_hist))
)

ranked = (
    scored
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    ranked
    .filter((~F.col("brand").eqNullSafe(F.lit("Stark"))) & (F.col("revenue") < 3400))
    .withColumn("running_sum_revenue_per_running_count_revenue", F.round((F.col("running_sum_revenue") / F.col("running_count_revenue")), 2))
)

with_dims_store_sales = (
    cleaned
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.store_id",
        "src.sale_date",
        "src.units",
        "src.revenue_band",
        "src.brand",
        "src.running_sum_revenue",
        "src.running_count_revenue",
        "src.cumulative_revenue",
        "src.running_sum_revenue_per_running_count_revenue",
        F.col("b.region"),
        F.col("b.city"),
    )
)

result = (
    with_dims_store_sales
    .groupBy("city", "brand")
    .agg(
        F.round(F.avg("cumulative_revenue"), 2).alias("avg_cumulative_revenue"),
        F.bit_or("units").alias("units_bit_or"),
        F.stddev_samp("units").alias("units_stddev"),
    )
)
