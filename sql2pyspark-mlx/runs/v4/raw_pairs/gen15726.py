from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

with_latest = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("payment_amount"))
)

result = (
    with_latest
    .groupBy("channel")
    .agg(
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.corr("shipping_fee", "amount").alias("corr_shipping_fee_amount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
