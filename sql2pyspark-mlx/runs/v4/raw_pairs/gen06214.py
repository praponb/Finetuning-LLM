from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded").desc_nulls_last())
w3 = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id")
w4 = Window.partitionBy("order_id", "product_id").orderBy(F.col("last_known_quantity").desc(), "id")
w5 = Window.partitionBy("order_id")
w6 = Window.partitionBy("order_id").orderBy(F.col("quantity_rounded").desc_nulls_first())
w7 = Window.partitionBy("order_id").orderBy(F.col("quantity_rounded_cume_dist").desc_nulls_first())

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_rounded", F.round("quantity", 2))
)

scored = (
    filtered
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w))
    .withColumn("quantity_rounded_cume_dist", F.cume_dist().over(w2))
    .withColumn("prev_quantity", F.lag("quantity", 1).over(w3))
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

remaining_keys = (
    deduped
    .select("id", "product_id")
    .exceptAll(
        df_order_items
        .filter(F.col("order_id").isin([8, 11, 17]))
        .select("id", "product_id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

filtered_order_items = (
    kept_rows
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_rounded_cume_dist_plus_quantity", (F.col("quantity_rounded_cume_dist") + F.col("quantity")))
    .withColumn("quantity_rounded_cume_dist_per_unit_price", F.round((F.col("quantity_rounded_cume_dist") / F.col("unit_price")), 2))
)

scored_order_items = (
    filtered_order_items
    .withColumn("order_id_stddev_samp_quantity_rounded_cume_dist", F.stddev_samp("quantity_rounded_cume_dist").over(w5))
    .withColumn("quantity_rounded_rank", F.rank().over(w6))
    .withColumn("quantity_rounded_cume_dist_cume_dist", F.cume_dist().over(w7))
)

with_dims = (
    scored_order_items
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.brand"))
)

result = (
    with_dims
    .groupBy("order_id")
    .pivot("brand", ["Acme", "Umbrella"])
    .agg(F.avg("quantity").alias("value_sum"))
    .select(
        "order_id",
        F.col("Acme").alias("acme"),
        F.col("Umbrella").alias("umbrella"),
    )
)
