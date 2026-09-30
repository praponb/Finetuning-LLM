from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 630)
    .select("id", "channel", "customer_id", "amount")
    .orderBy(F.col("id").desc())
)
