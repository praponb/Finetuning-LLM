from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.length("email") >= 5) & F.col("job_title").isNotNull())
    .groupBy("level")
    .agg(
        F.count("*").alias("num_employees"),
        F.min("hire_date").alias("min_hire_date"),
        F.sum("salary").alias("total_salary"),
        F.countDistinct("salary").alias("unique_levels"),
    )
    .filter(F.col("unique_levels") >= 2)
    .drop("unique_levels")
    .orderBy("total_salary")
)
