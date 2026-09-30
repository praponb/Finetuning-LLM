from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.level").isin(["L4", "L1"]) & (F.col("d.budget") <= 2909000))
    .select(
        F.col("d.dept_name"),
        F.col("e.hire_date"),
        F.col("e.level"),
        F.when(F.col("d.dept_name") == "Finance", 1).otherwise(0).alias("is_finance"),
    )
    .orderBy(F.col("hire_date").desc())
)
