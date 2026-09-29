from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "email", F.coalesce("segment", F.lit("N/A")).alias("segment_filled"))
    .orderBy("email", F.col("segment_filled").desc())
)
