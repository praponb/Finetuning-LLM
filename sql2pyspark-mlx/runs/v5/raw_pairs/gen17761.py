from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("dept_id").isin([3, 16, 26, 31]))
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_levels"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("D.unique_levels"),
        F.col("D.cnt"),
    )
)
