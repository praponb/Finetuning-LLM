from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("c.credit_limit") <= 10400)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("o.status"),
        F.col("c.credit_limit"),
        F.upper(F.col("o.channel")).alias("channel_upper"),
    )
    .orderBy("channel_upper", "status")
)
