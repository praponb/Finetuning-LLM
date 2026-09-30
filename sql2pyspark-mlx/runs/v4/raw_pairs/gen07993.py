from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price_rounded").desc_nulls_first())

cleaned = (
    df_order_items
    .filter(F.col("order_id").isin([2, 6, 28, 39]))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.round("unit_price", 2).alias("unit_price_rounded"),
    )
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

joined = (
    deduped
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.category"))
)

with_dims = (
    joined
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.stock_qty"), F.col("b.cost"), F.col("b.price"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("cost") >= 660, "LARGE").otherwise("SMALL").alias("cost_tier"),
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sum(F.when(F.col("category") == "Sports", F.col("unit_price")).otherwise(0)).alias("sports_unit_price"),
        F.sum(F.when(F.col("category") == "Fashion", F.col("price")).otherwise(0)).alias("fashion_price"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_rows"),
    )
    .filter(F.col("avg_cost") >= 0)
    .orderBy(F.col("sports_unit_price").desc_nulls_first())
)
