from pyspark.sql import functions as F

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-05-31").cast("date")) & F.col("region").isNotNull())
    .withColumn("sqft_level", F.when(F.col("sqft") >= 15763, "High").when(F.col("sqft") >= 3724, "Medium").otherwise("Low"))
)

result = (
    cleaned
    .select("city")
    .intersectAll(
        df_stores
        .filter(F.col("sqft") < 11998)
        .select("city")
    )
)
