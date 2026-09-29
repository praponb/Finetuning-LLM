from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["web", "store", "partner"]) & (F.col("channel").isin(["partner", "web", "store"]) | F.col("customer_id").isin([5, 16, 35])))
    .orderBy("status")
)
