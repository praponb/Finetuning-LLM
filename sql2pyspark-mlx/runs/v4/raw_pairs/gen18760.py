from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").isNotNull() & ((F.col("sqft") <= 11609) | (F.col("region") == "North")))
    .groupBy(F.month("opened_date").alias("opened_month"), "city")
    .agg(F.countDistinct("sqft").alias("unique_citys"))
    .orderBy("unique_citys")
)
