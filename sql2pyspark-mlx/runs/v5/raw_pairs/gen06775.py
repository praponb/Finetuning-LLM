from pyspark.sql import functions as F

recent_shipments = df_shipments.filter((F.col("order_id") == 2) | F.col("cost").between(83, 95))

agg = (
    recent_shipments
    .groupBy("carrier")
    .agg(F.max("cost").alias("max_cost"))
)

result = (
    agg
    .filter(F.col("max_cost") > 65)
    .select("carrier", "max_cost")
)
