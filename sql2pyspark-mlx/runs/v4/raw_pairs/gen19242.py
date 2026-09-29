from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc())
w2 = Window.orderBy(F.col("amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        "amount",
        "store_id",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.last("store_id").over(w2).alias("last_store_id"),
    )
)
