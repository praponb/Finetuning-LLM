from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc())

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        F.dense_rank().over(w).alias("shipping_fee_dense_rank"),
    )
)
