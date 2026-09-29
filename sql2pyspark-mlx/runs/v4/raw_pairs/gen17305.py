from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("channel", "status").orderBy(F.col("discount").desc(), "id")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("status", "store_id").orderBy(F.col("shipping_fee").asc_nulls_last())

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-04-16").cast("date")) & F.col("channel").isNotNull())
    .select("*", F.greatest("discount", "amount").alias("greatest_discount_amount"))
)

result = (
    filtered
    .select(
        "*",
        F.ntile(10).over(w_part).alias("discount_bucket"),
        F.corr("amount", "discount").over(w2).alias("corr_amount_discount"),
        F.dense_rank().over(w3).alias("shipping_fee_dense_rank"),
    )
)
