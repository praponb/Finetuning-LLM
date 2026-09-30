from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([29, 35]) & (F.lower(F.trim("email")) == "henry@startup.io"))
    .select(
        "email",
        F.when(F.col("salary") >= 160000, "High").when(F.col("salary") >= 42000, "Medium").otherwise("Low").alias("salary_band"),
    )
    .orderBy("salary_band")
)
