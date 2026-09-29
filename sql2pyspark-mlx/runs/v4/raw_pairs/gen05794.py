from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock_period").desc(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").asc_nulls_first(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("rn").desc(), "id")
shipping_fee_by_store_id = df_orders.groupBy("store_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-11-07")))
    .select("*", F.date_trunc("MONTH", "last_restock").alias("last_restock_period"))
)

ranked = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

latest_inventory = (
    latest
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_inventory
    .join(shipping_fee_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock_period",
        "rn",
        "latest_rank",
        F.col("min_order_shipping_fee"),
    )
    .orderBy("min_order_shipping_fee", "store_id")
)
