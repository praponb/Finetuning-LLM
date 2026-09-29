from pyspark.sql import functions as F

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-05-11").cast("date")) & (~F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .withColumn("segment_city", F.concat("segment", F.lit("/"), "city"))
)

result = (
    valid
    .groupBy("city")
    .pivot("country", ["SG", "JP", "DE"])
    .agg(F.sum("credit_limit"))
    .select(
        "city",
        F.col("SG").alias("sg"),
        F.col("JP").alias("jp"),
        F.col("DE").alias("de"),
    )
)
