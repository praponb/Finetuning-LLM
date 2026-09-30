from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") != 11376)
    .groupBy("region")
    .agg(F.count("*").alias("num_stores"))
    .filter(F.col("num_stores") < 6)
    .orderBy("num_stores")
)
