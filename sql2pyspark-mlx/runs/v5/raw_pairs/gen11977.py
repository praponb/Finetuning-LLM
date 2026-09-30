from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

base = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & F.col("shipping_fee").between(25, 32))
    .select("*", F.initcap(F.trim("channel")).alias("channel_title"))
)

result = (
    base
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.status",
        "s.amount",
        "s.shipping_fee",
        "s.channel_title",
        F.col("c.paid_at").alias("payment_paid_at"),
    )
)
