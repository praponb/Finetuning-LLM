from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") <= "2025-06-02") & F.col("manager_id").isin([15, 32, 35, 37]))
    .select(
        "id",
        "hire_date",
        "salary",
        F.date_format("hire_date", "yyyy-MM").alias("hire_date_label"),
    )
    .orderBy(F.col("salary").desc())
)
