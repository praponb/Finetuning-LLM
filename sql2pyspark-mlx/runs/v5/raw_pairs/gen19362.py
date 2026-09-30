from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.countDistinct("sqft").alias("unique_regions"),
        F.sum(F.when(F.col("region") == "North", 1).otherwise(0)).alias("north_count"),
        F.count("sqft").alias("sqft_count"),
    )
)
