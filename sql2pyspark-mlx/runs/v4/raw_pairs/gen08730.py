from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("d.location").isin(["New York", "Bangkok"]) | F.col("e.email").isNotNull())
    .select(
        F.col("d.budget"),
        F.col("d.location"),
        F.col("e.manager_id"),
        F.col("e.level"),
        F.col("e.is_remote"),
    )
)
