from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("manager_id") == 26) & F.col("level").isin(["L2", "L5"]) & F.col("dept_id").isin([17, 40]))
    .select(
        "id",
        "salary",
        F.trim("level").alias("clean_level"),
        F.upper(F.trim("email")).alias("email_normalized"),
    )
)
