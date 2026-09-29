from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc_nulls_last(), "id")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

latest = (
    df_orders
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "shipping_fee",
        "row_num",
        F.col("sum_inventory_quantity"),
    )
    .orderBy(F.col("store_id").desc_nulls_last())
)
