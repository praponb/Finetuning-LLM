from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("units_whole", "id")
w3 = Window.partitionBy("store_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc_nulls_last(), "id")

filtered = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-08-15").cast("date")) & (F.col("units") < 11))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
        F.ceil("units").alias("units_whole"),
    )
)

with_windows = (
    filtered
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("units_whole_bucket", F.ntile(3).over(w2))
)

scored = (
    with_windows
    .select(
        "id",
        "store_id",
        "revenue",
        "revenue_per_unit",
        "units_whole",
        "seq_no",
        F.max("revenue_per_unit").over(w3).alias("running_max_revenue_per_unit"),
        F.count("revenue_per_unit").over(w3).alias("running_count_revenue_per_unit"),
        F.min("revenue_per_unit").over(w3).alias("running_min_revenue_per_unit"),
        F.sum("revenue_per_unit").over(w3).alias("running_sum_revenue_per_unit"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "store_id",
        "revenue_per_unit",
        "seq_no",
        "running_max_revenue_per_unit",
        "running_min_revenue_per_unit",
        "running_sum_revenue_per_unit",
        "latest_rank",
    )
)
