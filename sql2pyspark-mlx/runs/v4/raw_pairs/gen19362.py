from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty")
w2 = Window.partitionBy("brand")
w3 = Window.partitionBy("category")
w4 = Window.orderBy(F.col("price").asc_nulls_first())
w5 = Window.partitionBy("supplier_id").orderBy(F.col("cost").asc_nulls_last())

matched_keys = (
    df_products
    .select("id", "supplier_id")
    .subtract(
        df_products
        .filter(F.col("price") <= 1030)
        .select("id", "supplier_id")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter((F.col("launch_date") > F.to_date(F.lit("2024-01-21"))) & F.col("supplier_id").isin([20, 23, 26]))
    .withColumn("product_name_brand_key", F.concat_ws("-", "product_name", "brand"))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("OTHER")))
)

metrics = (
    base
    .withColumn("stock_qty_dense_rank", F.dense_rank().over(w))
    .withColumn("brand_stddev_samp_price", F.stddev_samp("price").over(w2))
    .withColumn("brand_avg_stock_qty", F.avg("stock_qty").over(w2))
)

result = (
    metrics
    .select(
        "*",
        F.stddev_samp("price").over(w3).alias("category_stddev_samp_price"),
        F.percent_rank().over(w4).alias("price_percent_rank"),
        F.dense_rank().over(w5).alias("cost_dense_rank"),
    )
)
