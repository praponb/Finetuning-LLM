from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("region_filled", F.coalesce("region", F.lit("N/A")))
    .withColumn("sqft_rounded", F.round("sqft", 1))
)
