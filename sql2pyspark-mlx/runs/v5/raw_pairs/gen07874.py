from pyspark.sql import functions as F

filtered_stores = df_stores.filter(F.col("sqft").between(5337, 6616))

city_summary = (
    filtered_stores
    .groupBy("city")
    .agg(
        F.count("*").alias("num_stores"),
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
    )
)

result = (
    city_summary
    .filter(F.col("num_stores") < 5)
    .select("city", "num_stores")
    .orderBy(F.col("num_stores").desc(), "city")
    .limit(5)
)
