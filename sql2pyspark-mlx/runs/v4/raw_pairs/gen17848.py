from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id")
w_hist = Window.partitionBy("customer_id").orderBy(F.col("discount").desc())

result = (
    df_orders
    .withColumn("corr_discount_amount", F.corr("discount", "amount").over(w_part))
    .withColumn("discount_cume_dist", F.cume_dist().over(w_hist))
)
