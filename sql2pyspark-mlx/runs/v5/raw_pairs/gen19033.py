from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("running_avg_units").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_last())
w4 = Window.partitionBy("product_id")
w5 = Window.partitionBy("revenue_tier").orderBy(F.col("row_count").desc())

valid = (
    df_store_sales
    .filter(F.col("revenue") >= 2100)
    .select("*", F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"))
)

with_windows = (
    valid
    .filter(F.col("units") != 11)
    .select(
        "*",
        F.max("units").over(w_recent).alias("running_max_units"),
        F.avg("units").over(w_recent).alias("running_avg_units"),
        F.min("units").over(w_recent).alias("running_min_units"),
    )
)

with_windows_store_sales = (
    with_windows
    .select(
        "*",
        F.ntile(5).over(w2).alias("running_avg_units_bucket"),
        F.cume_dist().over(w3).alias("units_cume_dist"),
        F.avg("units").over(w4).alias("product_id_avg_units"),
        F.sum("revenue").over(w4).alias("product_id_sum_revenue"),
    )
)

cleaned = (
    with_windows_store_sales
    .filter(F.col("units_cume_dist").between(0.28, 0.44))
    .withColumn("units_cume_dist_level", F.when(F.col("units_cume_dist") >= 0.86, "High").when(F.col("units_cume_dist") >= 0.32, "Medium").otherwise("Low"))
    .withColumn("units_cume_dist_level_2", F.when(F.col("units_cume_dist") >= 0.9, "High").when(F.col("units_cume_dist") >= 0.12, "Medium").otherwise("Low"))
)

base = (
    cleaned
    .filter(F.col("product_id").isin([3, 9]))
    .withColumn("running_avg_units_bucket_tier", F.when(F.col("running_avg_units_bucket") >= 9, "High").when(F.col("running_avg_units_bucket") >= 2, "Medium").otherwise("Low"))
)

valid_store_sales = (
    base
    .filter(F.col("store_id") == 3)
    .withColumn("units_cume_dist_int", F.col("units_cume_dist").cast("int"))
    .withColumn("adjusted_running_avg_units", F.round((F.col("running_avg_units") * 1.15), 2))
)

result = (
    valid_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 3800, "PREMIUM").otherwise("STANDARD").alias("revenue_tier"),
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("running_avg_units_bucket", 0.5).alias("running_avg_units_bucket_p50"),
    )
    .filter(F.col("row_count") > 1)
    .withColumn("row_count_percent_rank", F.percent_rank().over(w5))
)
