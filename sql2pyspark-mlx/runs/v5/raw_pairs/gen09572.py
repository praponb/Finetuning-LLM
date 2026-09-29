from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc_nulls_last(), "id")
w_recent = Window.partitionBy("store_id").orderBy("revenue_per_unit", "id").rowsBetween(-6, Window.currentRow)
w_hist = Window.orderBy(F.col("revenue_per_unit").desc_nulls_first())

ranked = (
    df_store_sales
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

filtered = (
    ranked
    .filter(F.col("revenue") >= 1400)
    .select(
        "*",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
        F.when(F.col("revenue") >= 3600, "High").when(F.col("revenue") >= 1600, "Medium").otherwise("Low").alias("revenue_tier"),
    )
)

base = (
    filtered
    .filter(F.col("units") == 18)
    .select(
        "*",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "revenue_per_unit",
        "revenue_tier",
        F.count("units").over(w_recent).alias("last7_count_units"),
        F.sum("units").over(w_recent).alias("last7_sum_units"),
        F.max("units").over(w_recent).alias("last7_max_units"),
        F.cume_dist().over(w_hist).alias("revenue_per_unit_cume_dist"),
    )
)

result = (
    with_windows
    .alias("s")
    .join(df_products.alias("dim"), F.col("s.product_id") == F.col("dim.id"), "left")
    .rollup(F.col("dim.brand").alias("brand"), F.col("dim.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.revenue")), 2).alias("total_revenue"),
    )
    .orderBy(F.col("category").desc_nulls_first())
)
