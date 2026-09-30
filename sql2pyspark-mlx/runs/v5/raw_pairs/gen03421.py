from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").isNull())
    .select("order_date")
    .subtract(df_accounts.select(F.col("opened_date").alias("order_date")))
)
