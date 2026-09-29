from pyspark.sql import functions as F

min_amount = df_orders.agg(F.min("amount")).first()[0]

result = (
    df_orders
    .filter(F.col("amount") < min_amount)
    .select("id", "amount")
    .orderBy(F.col("id").desc())
)
