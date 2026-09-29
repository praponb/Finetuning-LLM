from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("revenue_per_unit").desc_nulls_first())
w4 = Window.orderBy("revenue", "id")
w5 = Window.partitionBy("product_id").orderBy("revenue_per_unit_percent_rank", "id")
w6 = Window.partitionBy("store_id").orderBy("cumulative_revenue_per_unit", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.orderBy("seq_no", "id")
w8 = Window.partitionBy("category").orderBy(F.col("revenue_share_avg_3").desc(), "id")
w9 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("store_id").orderBy("cumulative_revenue_per_unit", "id").rowsBetween(-3, -1)

valid = (
    df_store_sales
    .filter(F.col("sale_date") <= "2025-06-01")
    .select("*", (F.col("revenue") * F.col("units")).alias("revenue_per_unit"))
)

with_windows = (
    valid
    .select(
        "*",
        F.sum("revenue_per_unit").over(w).alias("cumulative_revenue_per_unit"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4).alias("revenue_share"),
        F.percent_rank().over(w3).alias("revenue_per_unit_percent_rank"),
    )
)

metrics = (
    with_windows
    .filter(F.col("revenue_share") >= 0.21)
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "revenue_per_unit",
        "cumulative_revenue_per_unit",
        F.ntile(5).over(w4).alias("revenue_bucket"),
        F.row_number().over(w5).alias("seq_no"),
        F.avg("revenue_share").over(w6).alias("revenue_share_avg_3"),
    )
)

enriched = (
    metrics
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.units",
        "e.revenue",
        "e.cumulative_revenue_per_unit",
        "e.revenue_bucket",
        "e.seq_no",
        "e.revenue_share_avg_3",
        F.col("d.brand"),
        F.col("d.category"),
        F.col("d.stock_qty"),
    )
)

scored = (
    enriched
    .withColumn("seq_no_bucket", F.ntile(3).over(w7))
    .withColumn("row_seq", F.row_number().over(w8))
)

result = (
    scored
    .select(
        "id",
        "units",
        "revenue",
        "cumulative_revenue_per_unit",
        "seq_no",
        "revenue_share_avg_3",
        "category",
        "stock_qty",
        "seq_no_bucket",
        "row_seq",
        F.sum("revenue_share_avg_3").over(w9).alias("running_revenue_share_avg_3"),
        F.avg("revenue_bucket").over(w10).alias("revenue_bucket_avg_3_prior"),
    )
)
