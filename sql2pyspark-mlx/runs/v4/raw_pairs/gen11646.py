from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "partner", "mobile"]))
    .select("id", "order_date", "shipping_fee", "store_id")
    .orderBy("order_date", F.col("store_id").desc())
)
