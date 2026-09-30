from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("email").like("%@outlook.com"))
    .select(
        "dept_id",
        "is_remote",
        "full_name",
        F.substring("level", 1, 2).alias("level_prefix"),
    )
)
