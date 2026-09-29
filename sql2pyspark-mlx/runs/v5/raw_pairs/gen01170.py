from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["web", "partner", "store"]))
    .select("id", "discount", "shipping_fee", "channel")
    .orderBy("shipping_fee")
)
