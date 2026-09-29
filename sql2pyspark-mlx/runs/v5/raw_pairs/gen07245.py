from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy("revenue", "id")
w3 = Window.partitionBy("product_id").orderBy("units")
w4 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy(F.col("last_known_revenue").desc_nulls_first(), "id")

joined = (
    df_store_sales
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.launch_date"), F.col("d.product_name"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.price"))
)

metrics = (
    with_dims
    .select(
        "*",
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 4).alias("revenue_share"),
        F.ntile(5).over(w2).alias("revenue_bucket"),
        F.ntile(4).over(w2).alias("revenue_bucket_2"),
        F.rank().over(w3).alias("units_rank"),
        F.last("revenue", ignorenulls=True).over(w4).alias("last_known_revenue"),
    )
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .groupBy("product_id")
    .agg(
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.percentile_approx("revenue_bucket_2", [0.25, 0.5, 0.75]).alias("revenue_bucket_2_quartiles"),
    )
    .orderBy(F.col("product_id").desc())
)
