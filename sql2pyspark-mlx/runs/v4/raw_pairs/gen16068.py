from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_first(), "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isNull())
    .select(
        "*",
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
    )
)

result = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
