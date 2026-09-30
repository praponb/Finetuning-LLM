from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc_nulls_last(), "id")
w_ordered = Window.partitionBy("store_id")
w3 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("units", "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("units_bucket").desc(), "id")
w6 = Window.partitionBy("running_revenue_tier").orderBy(F.col("running_revenue_p95").desc())

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-04-07").cast("date"))
    .select("*", F.coalesce("units", F.lit(0)).alias("units_filled"))
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    latest
    .filter(F.col("revenue").between(2500, 3800))
    .select(
        "*",
        F.corr("revenue", "units").over(w_ordered).alias("corr_revenue_units"),
        F.sum("revenue").over(w3).alias("running_revenue"),
        F.ntile(3).over(w4).alias("units_bucket"),
    )
)

valid = (
    metrics
    .filter(F.col("product_id").isin([3, 8, 9, 10]))
    .select(
        "*",
        (F.col("units_bucket") + F.col("corr_revenue_units")).alias("units_bucket_plus_corr_revenue_units"),
    )
)

latest_store_sales = (
    valid
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_id", "product_id", "revenue", "rn", "running_revenue")
)

result = (
    latest_store_sales
    .groupBy(
        F.when(F.col("running_revenue") >= 19872, "LARGE").otherwise("SMALL").alias("running_revenue_tier"),
        "store_id",
    )
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.percentile_approx("running_revenue", 0.95).alias("running_revenue_p95"),
    )
    .withColumn("running_revenue_p95_percent_rank", F.percent_rank().over(w6))
)
