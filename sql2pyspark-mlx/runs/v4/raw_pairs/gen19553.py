from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(460, 950))
    .select(F.col("price").alias("value"))
    .subtract(df_orders.select(F.col("amount").alias("value")))
    .orderBy("value")
)
