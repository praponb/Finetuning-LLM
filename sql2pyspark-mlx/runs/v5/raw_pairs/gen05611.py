from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("A%") & (F.col("sqft") > 9925))
    .groupBy(F.year("opened_date").alias("opened_year"), "city", "region")
    .agg(
        F.countDistinct("sqft").alias("unique_regions"),
        F.count("*").alias("num_stores"),
    )
)
