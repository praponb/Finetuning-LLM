from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc(), "id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
    )
)

result = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_id", "product_id")
)
