from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "region",
        F.when(F.col("city").isin(["Singapore", "New York"]), "core").otherwise("other").alias("city_group"),
    )
    .agg(
        F.sum(F.when(F.col("region") == "Central", F.col("sqft")).otherwise(0)).alias("central_sqft"),
    )
    .orderBy("city_group")
)
