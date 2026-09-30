from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 1600)
    .select("channel")
    .distinct()
    .orderBy("channel")
    .limit(5)
)
