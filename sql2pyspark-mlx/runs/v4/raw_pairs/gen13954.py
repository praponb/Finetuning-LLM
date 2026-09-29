from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("country").isin(["UK", "JP"]))
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        "is_active",
    )
    .agg(F.countDistinct("age").alias("unique_segments"))
    .filter(F.col("unique_segments") > 6)
    .orderBy("signup_cohort", F.col("unique_segments").desc())
)
