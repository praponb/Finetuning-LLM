from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price").desc_nulls_last())

latest = (
    df_order_items
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "product_id", "quantity")
)

result = (
    latest
    .cube("product_id", "order_id")
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
    .orderBy(F.col("product_id").desc_nulls_first())
)
