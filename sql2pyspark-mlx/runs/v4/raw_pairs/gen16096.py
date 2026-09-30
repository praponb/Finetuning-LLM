from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("cart_value", "id")

parsed = (
    df_carts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.col("ship_to.zip").alias("ship_zip"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.ntile(10).over(w).alias("cart_value_bucket"),
        F.ntile(10).over(w).alias("cart_value_bucket_2"),
        F.ntile(10).over(w).alias("cart_value_bucket_3"),
    )
)
