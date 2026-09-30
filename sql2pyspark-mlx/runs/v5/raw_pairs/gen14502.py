from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.when(F.col("cost") >= 590, "high").when(F.col("cost") >= 360, "mid").otherwise("low").alias("cost_bucket"))
    .pivot("brand", ["Stark", "Initech"])
    .agg(F.count("cost"))
    .orderBy("cost_bucket")
)
