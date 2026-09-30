from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(12597, 14704))
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        "city",
    )
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("sqft_count") < 6)
    .select("opened_cohort", "city", "sqft_count", "min_sqft")
)
