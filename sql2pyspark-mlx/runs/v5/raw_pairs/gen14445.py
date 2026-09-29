from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") > 1280) & F.col("channel").isNull())
    .select("id", "shipping_fee")
    .orderBy(F.col("id").desc())
    .limit(50)
)
