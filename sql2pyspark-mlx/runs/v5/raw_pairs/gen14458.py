from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price_int").desc(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("quantity") != 4))
    .select(
        "id",
        "order_id",
        "product_id",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

result = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "product_id")
)
