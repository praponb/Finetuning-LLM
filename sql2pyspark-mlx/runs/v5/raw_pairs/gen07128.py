from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("store_id")
w4 = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue").desc_nulls_first(), "id")
w6 = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id")
w7 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())
w8 = Window.partitionBy("revenue_tier").orderBy(F.col("distinct_ids").desc())

enriched = (
    df_store_sales
    .withColumn("revenue_bucket", F.ntile(3).over(w))
    .withColumn("cumulative_units", F.sum("units").over(w2))
    .withColumn("store_id_stddev_samp_revenue", F.stddev_samp("revenue").over(w_part))
    .withColumn("revenue_avg_2", F.avg("revenue").over(w4))
)

joined = (
    enriched
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.revenue",
        "src.cumulative_units",
        F.col("b.stock_qty"),
    )
)

deduped = (
    joined
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

with_windows = (
    deduped
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "revenue",
        "stock_qty",
        "rn",
        F.ntile(3).over(w6).alias("revenue_bucket"),
        F.cume_dist().over(w7).alias("revenue_cume_dist"),
        F.round((F.col("cumulative_units") / F.sum("cumulative_units").over(w_part)), 4).alias("cumulative_units_share"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("revenue") >= 4400, "PREMIUM").otherwise("STANDARD").alias("revenue_tier"),
        F.when(F.col("revenue_cume_dist") >= 0.78, "LARGE").otherwise("SMALL").alias("revenue_cume_dist_tier"),
    )
    .agg(
        F.min("cumulative_units_share").alias("min_cumulative_units_share"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .withColumn("distinct_ids_rank", F.rank().over(w8))
)
