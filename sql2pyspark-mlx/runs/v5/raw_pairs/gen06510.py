from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["SG", "US"])
    .agg(F.max("rating"))
    .select("city", F.col("SG").alias("sg"), F.col("US").alias("us"))
)
