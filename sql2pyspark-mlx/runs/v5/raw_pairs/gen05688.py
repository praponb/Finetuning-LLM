from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") > "2024-01-01")
    .withColumn("city_group", F.when(F.col("city").isin(["Berlin", "Singapore"]), "Priority").otherwise("Other"))
    .withColumn("sqft_rounded", F.round("sqft", 1))
)
