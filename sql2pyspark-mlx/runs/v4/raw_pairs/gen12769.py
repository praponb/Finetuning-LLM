from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount_with_shipping").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("amount_with_shipping", "id").rowsBetween(-2, -1)

base = (
    df_orders
    .filter(~F.col("channel").isin(["partner", "mobile", "web"]))
    .select(
        "*",
        (F.col("amount") - F.col("shipping_fee")).alias("amount_with_shipping"),
    )
)

result = (
    base
    .withColumn("amount_with_shipping_bucket", F.ntile(10).over(w))
    .withColumn("discount_avg_2_prior", F.avg("discount").over(w2))
)
