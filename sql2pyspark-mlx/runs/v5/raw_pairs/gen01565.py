from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").asc_nulls_last())

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.col("quantity").cast("string").alias("quantity_str"),
    )
)

result = (
    cleaned
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)
