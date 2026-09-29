from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc_nulls_first(), "id")

valid = (
    df_order_items
    .filter(F.col("unit_price").between(150, 320))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
    )
)

result = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "product_id", "unit_price", "line_total")
)
