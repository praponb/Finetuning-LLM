from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("sqft_filled", F.coalesce("sqft", F.lit(0)))
    .orderBy(F.col("city").desc())
)
