from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["SG", "JP"]))
    .groupBy("country")
    .agg(F.count("rating").alias("rating_count"))
    .filter(F.col("rating_count") < 8)
    .select("country", "rating_count")
    .orderBy("rating_count")
)
