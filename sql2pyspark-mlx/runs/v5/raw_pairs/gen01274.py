from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("units", "id")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("store_id").orderBy(F.col("prev_units").desc(), "id")
w4 = Window.orderBy("prev_units", "id")

with_windows = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        F.lag("units", 1).over(w).alias("prev_units"),
        F.corr("revenue", "units").over(w2).alias("corr_revenue_units"),
    )
)

filtered = (
    with_windows
    .filter(F.col("units") <= 22)
    .withColumn("corr_revenue_units_per_revenue", F.round((F.col("corr_revenue_units") / F.col("revenue")), 2))
)

with_windows_store_sales = (
    filtered
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("prev_corr_revenue_units_per_revenue", F.lag("corr_revenue_units_per_revenue", 1, 0).over(w4))
)

base = (
    with_windows_store_sales
    .filter(F.col("row_seq") >= 7)
    .withColumn("prev_corr_revenue_units_per_revenue_plus_prev_units", (F.col("prev_corr_revenue_units_per_revenue") + F.col("prev_units")))
    .withColumn("prev_corr_revenue_units_per_revenue_int", F.col("prev_corr_revenue_units_per_revenue").cast("int"))
)

result = (
    base
    .groupBy(
        "store_id",
        F.when(F.col("corr_revenue_units") >= 0.58, "HIGH").otherwise("LOW").alias("corr_revenue_units_tier"),
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.avg("revenue").alias("avg_revenue"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
)
