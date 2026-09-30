from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("opened_date_label", F.date_format("opened_date", "MMM yyyy"))
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 15784, "High").when(F.col("sqft") >= 5788, "Medium").otherwise("Low"))
)
