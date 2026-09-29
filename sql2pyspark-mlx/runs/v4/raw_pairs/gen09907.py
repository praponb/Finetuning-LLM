from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

base = (
    df_order_items
    .filter(F.col("order_id").isin([28, 35]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.col("quantity").cast("double").alias("quantity_str"),
    )
)

result = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
