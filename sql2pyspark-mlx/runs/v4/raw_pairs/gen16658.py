from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull())
    .groupBy("segment")
    .agg(F.countDistinct("age").alias("unique_segments"))
    .filter(F.col("unique_segments") > 4)
    .select("segment", "unique_segments")
)
