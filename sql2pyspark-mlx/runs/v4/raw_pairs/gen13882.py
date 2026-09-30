from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.salary") > 130000)
    .groupBy(F.col("d.dept_name"))
    .agg(
        F.sum(F.when(F.col("e.level") == "L3", F.col("e.salary")).otherwise(0)).alias("l3_salary"),
    )
)
