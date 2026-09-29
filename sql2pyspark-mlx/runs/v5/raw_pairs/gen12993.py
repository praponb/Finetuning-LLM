from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") != "L2")
    .select(
        "id",
        "is_remote",
        "level",
        F.when(F.col("salary") >= 138000, "High").when(F.col("salary") >= 45000, "Medium").otherwise("Low").alias("salary_tier"),
    )
    .orderBy("id")
)
