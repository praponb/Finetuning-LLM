from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("order_id").orderBy("quantity", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("rn").desc(), "id")
w4 = Window.partitionBy("product_id")

enriched = (
    df_order_items
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.product_name"))
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    latest
    .withColumn("prev_quantity", F.lag("quantity", 2).over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)

with_windows_order_items = (
    with_windows
    .select(
        "*",
        F.round((F.col("unit_price") / F.sum("unit_price").over(w4)), 4).alias("unit_price_share"),
        F.avg("quantity").over(w4).alias("product_id_avg_quantity"),
    )
)

result = (
    with_windows_order_items
    .cube(
        F.when(F.col("unit_price_share") >= 0.64, "PREMIUM").otherwise("STANDARD").alias("unit_price_share_tier"),
        "product_id",
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.bit_and("quantity").alias("quantity_bit_and_2"),
        F.max("product_id_avg_quantity").alias("max_product_id_avg_quantity"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("unit_price_share", 0.9).alias("unit_price_share_p90"),
    )
    .filter(F.col("quantity_bit_and") < 1)
)
