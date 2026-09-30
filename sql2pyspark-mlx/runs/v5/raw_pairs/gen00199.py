from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price_whole").desc(), "id")

base = (
    df_order_items
    .filter(F.col("quantity") == 5)
    .select(
        "id",
        "order_id",
        "product_id",
        F.ceil("unit_price").alias("unit_price_whole"),
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)
