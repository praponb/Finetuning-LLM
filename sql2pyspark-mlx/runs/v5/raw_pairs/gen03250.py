from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue_filled").asc_nulls_last())
w3 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("running_revenue_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-06-30").cast("date")) & (F.col("units") > 28))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
    )
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    latest
    .filter(F.col("sale_date") <= "2024-12-01")
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        F.cume_dist().over(w2).alias("revenue_filled_cume_dist"),
        F.sum("revenue_filled").over(w3).alias("running_revenue_filled"),
    )
)

scored = (
    with_windows
    .withColumn("running_max_revenue", F.max("revenue").over(w4))
    .withColumn("running_min_revenue", F.min("revenue").over(w4))
    .withColumn("running_sum_revenue", F.sum("revenue").over(w4))
    .withColumn("running_avg_revenue", F.avg("revenue").over(w4))
)

with_dims = (
    scored
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.category"), F.col("p.product_name"), F.col("p.stock_qty"))
)

result = (
    with_dims
    .select(
        "id",
        "product_id",
        "revenue",
        "revenue_filled_cume_dist",
        "running_revenue_filled",
        "running_max_revenue",
        "running_min_revenue",
        "running_sum_revenue",
        "running_avg_revenue",
    )
)
