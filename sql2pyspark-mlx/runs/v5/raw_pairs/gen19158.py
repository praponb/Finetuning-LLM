from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc_nulls_first())
w4 = Window.orderBy(F.col("revenue").asc_nulls_first())
w_hist = Window.partitionBy("product_id").orderBy("revenue", "id")

cleaned = (
    df_store_sales
    .filter(F.col("revenue") >= 1400)
    .select("*", F.round("revenue", 1).alias("revenue_rounded"))
)

enriched = (
    cleaned
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        F.ntile(10).over(w).alias("units_bucket"),
        F.last("revenue_rounded", ignorenulls=True).over(w2).alias("last_known_revenue_rounded"),
        F.cume_dist().over(w3).alias("revenue_cume_dist"),
        F.cume_dist().over(w4).alias("revenue_cume_dist_2"),
        F.lag("units", 2, 0).over(w_hist).alias("prev_units"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.last_known_revenue_rounded",
        "src.revenue_cume_dist",
        F.col("b.product_name"),
    )
)

valid = (
    with_dims
    .filter(F.col("product_name").isNotNull())
    .select(
        "id",
        "store_id",
        "last_known_revenue_rounded",
        "revenue_cume_dist",
        "product_name",
        (F.col("revenue_cume_dist") * F.col("last_known_revenue_rounded")).alias("revenue_cume_dist_times_last_known_revenue_rounded"),
    )
)

result = (
    valid
    .withColumn("revenue_cume_dist_tier", F.when(F.col("revenue_cume_dist") >= 0.64, "HIGH").otherwise("LOW"))
    .withColumn("revenue_cume_dist_times_last_known_revenue_rounded_tier", F.when(F.col("revenue_cume_dist_times_last_known_revenue_rounded") >= 83, "LARGE").otherwise("SMALL"))
    .rollup(
        "revenue_cume_dist_tier",
        "revenue_cume_dist_times_last_known_revenue_rounded_tier",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("last_known_revenue_rounded"), 2).alias("avg_last_known_revenue_rounded"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.max("last_known_revenue_rounded").alias("max_last_known_revenue_rounded"),
        F.grouping("revenue_cume_dist_tier").alias("grouping_id"),
    )
    .filter(F.col("distinct_store_ids") > 1)
)
