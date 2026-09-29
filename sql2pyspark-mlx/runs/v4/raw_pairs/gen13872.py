from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.col("segment") == "SMB").select("id")

result = (
    df_orders.alias("o")
    .join(excluded_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("o.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .filter(F.col("o.status") != "CANCELLED")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.channel"),
        F.col("o.store_id"),
    )
)
