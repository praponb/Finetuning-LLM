from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") <= 0.24) | F.col("channel").isin(["store", "mobile"]))
    .select("amount")
    .orderBy("amount")
)
