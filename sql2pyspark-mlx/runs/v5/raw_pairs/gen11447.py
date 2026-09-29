from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") < 45000)
    .select(F.col("opened_date").alias("event_date"))
    .unionByName(df_orders.select(F.col("order_date").alias("event_date")))
    .distinct()
)
