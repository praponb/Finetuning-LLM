from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Tokyo", "Bangkok"])
    .agg(F.max("sqft").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "region",
        F.col("Tokyo_amount").alias("tokyo_amount"),
        F.col("Tokyo_n").alias("tokyo_n"),
        F.col("Bangkok_amount").alias("bangkok_amount"),
        F.col("Bangkok_n").alias("bangkok_n"),
    )
)
