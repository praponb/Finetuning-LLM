from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price")

base = (
    df_order_items
    .filter(F.col("order_id").isin([1, 2, 15, 39]))
    .select(
        "*",
        F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 2, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

filtered = (
    base
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped_order_items = (
    deduped
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped_order_items
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .rollup(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("a.quantity_filled")).alias("total_quantity_filled"),
        F.grouping_id().alias("level_id"),
    )
)
