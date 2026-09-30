from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").asc_nulls_last())
w2 = Window.partitionBy("customer_id").orderBy(F.col("amount").desc_nulls_first())

result = (
    df_orders
    .filter(F.col("store_id") == 8)
    .select(
        "*",
        F.dense_rank().over(w).alias("shipping_fee_dense_rank"),
        F.cume_dist().over(w2).alias("amount_cume_dist"),
    )
)
