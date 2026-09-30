from pyspark.sql import functions as F

src = (
    df_orders
    .filter(F.col("order_date") <= "2024-01-01")
    .withColumn("order_date_plus_67d", F.date_add("order_date", 67))
)

agg = (
    src
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
)

result = (
    agg
    .filter(F.col("cancelled_count") < 51)
    .select("status", "cancelled_count")
)
