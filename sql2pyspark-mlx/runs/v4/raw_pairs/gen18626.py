from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc_nulls_last(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity_rounded").asc_nulls_first())
w3 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

ranked = (
    df_order_items
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_dims = (
    ranked
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.stock_qty"))
)

base = (
    with_dims
    .filter(F.col("product_id").isNotNull())
    .select("*", F.round("quantity", 0).alias("quantity_rounded"))
)

scored = (
    base
    .withColumn("quantity_rounded_dense_rank", F.dense_rank().over(w2))
    .withColumn("quantity_bucket", F.ntile(5).over(w3))
)

grouped = (
    scored
    .groupBy("order_id")
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("quantity_rounded_dense_rank", [0.25, 0.5, 0.75]).alias("quantity_rounded_dense_rank_quartiles"),
        F.max("quantity_bucket").alias("max_quantity_bucket"),
        F.round(F.avg("quantity_bucket"), 2).alias("avg_quantity_bucket"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .select(
        "order_id",
        "quantity_bit_and",
        "max_quantity_bucket",
        "avg_quantity_bucket",
    )
    .orderBy(F.col("max_quantity_bucket").desc())
)
