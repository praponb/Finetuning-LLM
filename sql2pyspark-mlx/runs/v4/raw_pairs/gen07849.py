from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc_nulls_last(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)

result = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
