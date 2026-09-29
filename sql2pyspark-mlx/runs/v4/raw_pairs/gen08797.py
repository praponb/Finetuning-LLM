from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("discount").desc())
w2 = Window.partitionBy("customer_id", "store_id").orderBy("discount", "id").rowsBetween(-2, -1)

result = (
    df_orders
    .select(
        "*",
        F.percent_rank().over(w_part).alias("discount_percent_rank"),
        F.avg("shipping_fee").over(w2).alias("shipping_fee_avg_2_prior"),
    )
)
