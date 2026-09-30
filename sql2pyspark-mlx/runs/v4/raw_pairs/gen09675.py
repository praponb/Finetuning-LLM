from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([26, 29]) & F.col("dept_id").isin([12, 26, 32, 40]))
    .select(
        "id",
        "salary",
        "level",
        F.lower("full_name").alias("full_name_lower"),
        F.round((F.col("salary") * 0.9), 2).alias("salary_scaled"),
    )
    .orderBy("level", "id")
    .limit(20)
)
