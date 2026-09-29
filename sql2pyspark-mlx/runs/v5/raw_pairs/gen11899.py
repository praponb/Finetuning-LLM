from pyspark.sql import functions as F

max_discount = df_orders.agg(F.max("discount")).first()[0]

result = (
    df_orders
    .filter(F.col("discount") > max_discount)
    .select("id", "discount", "channel", "customer_id")
    .orderBy(F.col("customer_id").desc(), "channel", "id")
    .limit(10)
)
