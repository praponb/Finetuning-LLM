from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("launch_date").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("revenue").asc_nulls_first(), "id")

joined = (
    df_store_sales
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.sale_date",
        "a.revenue",
        F.col("p.product_name"),
        F.col("p.launch_date"),
    )
)

ranked = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "store_id", "revenue", "product_name", "launch_date")
)

ranked_store_sales = (
    ranked
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_dims = (
    ranked_store_sales
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "inner")
    .filter(F.col("b.city") == "London")
    .select(
        "src.id",
        "src.store_id",
        "src.revenue",
        "src.product_name",
        F.col("b.store_name"),
        F.col("b.sqft"),
    )
)

result = (
    with_dims
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 17803, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["sqft_tier", "store_id"], ["sqft_tier"], []],
        "sqft_tier",
        "store_id",
    )
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.min("revenue").alias("min_revenue"),
        F.percentile_approx("revenue", 0.5).alias("revenue_p50"),
        F.max("sqft").alias("max_sqft"),
    )
    .orderBy(F.col("avg_revenue").desc())
)
