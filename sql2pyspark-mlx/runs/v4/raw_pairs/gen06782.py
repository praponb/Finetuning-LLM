from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.year("hire_date").alias("hire_year"),
        F.when(F.col("salary") >= 122000, "high").when(F.col("salary") >= 54000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
        F.countDistinct("salary").alias("unique_levels"),
    )
    .filter(F.col("unique_levels") < 4)
)
