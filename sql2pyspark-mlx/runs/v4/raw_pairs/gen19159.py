from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(F.count("cost").alias("cost_count"))
    .filter(F.col("cost_count") >= 3)
    .select("order_id", "cost_count")
    .orderBy(F.col("order_id").desc(), F.col("cost_count").desc())
)
