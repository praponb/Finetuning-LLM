from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.filter(F.col("signup_date").between("2023-08-22", "2023-09-02")).alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left_semi")
    .select(F.col("orders.id"), F.col("orders.discount"), F.col("orders.store_id"))
)
