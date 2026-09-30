from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded").desc_nulls_last(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_rounded", F.round("quantity", 0))
)

latest = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest_order_items = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

latest_order_items_2 = (
    latest_order_items
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest_order_items_2
    .groupBy("order_id")
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.count("*").alias("num_rows"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
    .orderBy(F.col("distinct_order_ids").asc_nulls_last())
)
