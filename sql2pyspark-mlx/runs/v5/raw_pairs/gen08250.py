from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w_part = Window.orderBy(F.col("quantity").desc(), "id")
discount_by_store_id = df_orders.groupBy("store_id").agg(F.min("discount").alias("min_order_discount"))

metrics = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5, 6]))
    .withColumn("quantity_bucket", F.ntile(3).over(w))
    .withColumn("row_seq", F.row_number().over(w_part))
)

result = (
    metrics
    .join(discount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "quantity_bucket",
        "row_seq",
        F.col("min_order_discount"),
    )
)
