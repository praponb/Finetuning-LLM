from pyspark.sql import functions as F

result = (
    df_stores
    .filter(((F.col("region") == "North") & (F.col("sqft") == 14324)) | F.col("store_name").isNotNull())
    .groupBy(F.month("opened_date").alias("opened_month"), "city")
    .agg(
        F.countDistinct("sqft").alias("unique_regions"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
    .filter(F.col("pct_chiang_mai") >= 47)
    .orderBy(F.col("city").desc())
)
