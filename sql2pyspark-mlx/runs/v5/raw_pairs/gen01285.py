from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") <= 920)
    .withColumn("amount_adjusted", F.round((F.col("amount") * 100), 2))
    .orderBy(F.col("order_date").desc(), "id")
    .limit(20)
)
