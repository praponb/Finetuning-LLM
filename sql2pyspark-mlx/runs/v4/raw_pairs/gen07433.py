from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last(), "id")
w3 = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc(), "id")

ranked = (
    df_order_items
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked_order_items = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked_order_items
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("quantity") >= 8, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
        "order_id",
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.count("*").alias("num_rows"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
    .orderBy(F.col("quantity_bit_and").asc_nulls_first())
)
