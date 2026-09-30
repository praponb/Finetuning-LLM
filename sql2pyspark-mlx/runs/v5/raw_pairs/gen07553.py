from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc_nulls_first(), "id")

deduped = (
    df_order_items
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("unit_price") >= 420, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
        F.when(F.col("quantity") >= 9, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("quantity").alias("min_quantity"),
        F.count("*").alias("event_count"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.bit_and("quantity").alias("quantity_bit_and_2"),
    )
    .orderBy(F.col("quantity_tier").desc())
)
