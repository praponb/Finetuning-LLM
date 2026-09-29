from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") < 85000) & (F.col("full_name").isNotNull() | F.col("email").isNotNull()))
    .select(
        "id",
        "email",
        F.when(F.col("job_title") == "Designer", 1).otherwise(0).alias("is_designer"),
    )
    .orderBy(F.col("is_designer").desc())
)
