from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.filter(~F.col("is_active")).alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left_semi")
    .filter(F.col("orders.amount") >= 1250)
    .select(F.col("orders.id"), F.col("orders.discount"), F.col("orders.amount"))
    .orderBy(F.col("id").desc(), "discount")
)
