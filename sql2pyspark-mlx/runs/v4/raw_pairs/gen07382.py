from pyspark.sql import functions as F

valid_shipments = df_shipments.filter(F.col("order_id") == 30)

agg = (
    valid_shipments
    .groupBy("order_id", F.year("shipped_date").alias("shipped_year"))
    .agg(F.min("cost").alias("min_cost"))
)

result = (
    agg
    .filter(F.col("min_cost") >= 93)
    .select("order_id", "shipped_year")
    .orderBy(F.col("order_id").desc())
)
