from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
        "is_remote",
    )
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
    )
    .filter(F.col("l2_count") >= 6)
    .select("hire_cohort", "is_remote", "last_hire_date", "l2_count")
    .orderBy(F.col("l2_count").desc())
)
