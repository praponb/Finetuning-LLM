from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("segment").isin(["Consumer", "SMB"]), "priority").otherwise("other").alias("segment_group"),
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(F.sum("age").alias("total_age"))
    .filter(F.col("total_age") < 360)
    .orderBy("segment_group", "signup_cohort")
    .limit(50)
)
