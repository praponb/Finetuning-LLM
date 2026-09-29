from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").asc_nulls_last(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") > 130))
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)
