from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

filtered = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("status") == "SHIPPED"))
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.ceil("shipping_fee").alias("shipping_fee_whole"),
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.method"))
)
