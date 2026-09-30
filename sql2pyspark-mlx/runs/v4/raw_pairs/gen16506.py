from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(F.count("*").alias("num_employees"))
    .filter(F.col("num_employees") > 9)
    .orderBy("num_employees", F.col("hire_cohort").desc())
)
