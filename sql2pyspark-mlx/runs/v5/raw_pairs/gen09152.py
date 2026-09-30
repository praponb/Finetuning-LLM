from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") > 6065) & F.col("sqft").between(8906, 12095))
    .groupBy(
        "city",
        "region",
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") < 5900)
    .orderBy(F.col("min_sqft").desc())
)
