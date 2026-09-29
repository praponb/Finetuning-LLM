from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(~F.col("d.location").isin(["London", "Singapore", "New York"]))
    .select(
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("e.is_remote"),
        F.trim(F.col("e.level")).alias("clean_level"),
    )
)
