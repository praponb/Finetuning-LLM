from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .groupBy(F.quarter(F.col("o.order_date")).alias("order_quarter"))
    .agg(F.sum(F.when(F.col("p.method") == "card", 1).otherwise(0)).alias("card_count"))
    .filter(F.col("card_count") >= 32)
    .orderBy(F.col("card_count").desc())
)
