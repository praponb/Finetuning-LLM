from pyspark.sql import functions as F

result = (
    df_stores
    .cube(
        F.when(F.col("sqft") >= 17753, "LARGE").otherwise("SMALL").alias("sqft_tier"),
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
        "city",
    )
    .agg(
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.sum(F.when(F.col("region") == "South", F.col("sqft")).otherwise(0)).alias("south_sqft"),
    )
    .orderBy(F.col("sqft_bit_or").asc_nulls_last())
)
