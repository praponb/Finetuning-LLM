from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([19, 35]))
    .select(
        "dept_id",
        F.when(F.col("salary") >= 120000, "High").when(F.col("salary") >= 84000, "Medium").otherwise("Low").alias("salary_tier"),
    )
)
