from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("product_stock_qty").desc_nulls_last(), "id")
w3 = Window.partitionBy("city").orderBy("dedup_rank", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("rating").desc())
w5 = Window.partitionBy("city", "country")
w6 = Window.partitionBy("country", "city").orderBy("product_stock_qty", "id")
w7 = Window.orderBy("product_price")
w8 = Window.partitionBy("city").orderBy("rating", "id")

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.price").alias("product_price"),
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.cost").between(390, 580)), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category").isin(["Electronics", "Grocery"])), "left_semi")
)

latest = (
    eligible
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    latest
    .filter(F.col("supplier_name").isNotNull())
    .withColumn("product_stock_qty_avg_3", F.avg("product_stock_qty").over(w3))
    .withColumn("rating_cume_dist", F.cume_dist().over(w4))
    .withColumn("city_sum_product_price", F.sum("product_price").over(w5))
    .withColumn("product_stock_qty_bucket", F.ntile(10).over(w6))
)

enriched = (
    scored
    .select(
        "*",
        F.dense_rank().over(w7).alias("product_price_dense_rank"),
        F.lead("product_stock_qty_avg_3", 2).over(w8).alias("next_product_stock_qty_avg_3"),
        F.ntile(5).over(w6).alias("product_stock_qty_bucket_2"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("city_sum_product_price") >= 5601, "LARGE").otherwise("SMALL").alias("city_sum_product_price_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
        F.avg("product_stock_qty_bucket_2").alias("avg_product_stock_qty_bucket_2"),
        F.round(F.sum("product_price_dense_rank") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("product_price_dense_rank_per_chiang_mai"),
    )
    .filter(F.col("singapore_count") >= 1)
    .orderBy(F.col("avg_product_stock_qty_bucket_2").desc())
)
