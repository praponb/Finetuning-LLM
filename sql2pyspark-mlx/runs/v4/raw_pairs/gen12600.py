from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(F.countDistinct("amount").alias("unique_statuss"))
    .filter(F.col("unique_statuss") >= 7)
    .select("status", "unique_statuss")
    .orderBy("status", F.col("unique_statuss").desc())
    .limit(3)
)
