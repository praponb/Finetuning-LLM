from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").asc_nulls_last(), "id")

ranked = (
    df_order_items
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy("order_id", "product_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.min("unit_price").alias("min_unit_price"),
        F.count("*").alias("event_count"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price_2"),
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
    )
    .filter(F.col("avg_unit_price_2") < 1000000)
)
