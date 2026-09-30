from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.city") == "Bangkok") & (F.col("o.order_date") > "2023-09-01"))
    .select(
        F.col("c.segment"),
        F.col("c.signup_date"),
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        F.col("o.status"),
    )
    .orderBy("status")
)
