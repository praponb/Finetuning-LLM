from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc())

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        "amount",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 1)
    .select("id", "customer_id", "shipping_fee", "amount")
    .orderBy(F.col("customer_id").desc())
)
