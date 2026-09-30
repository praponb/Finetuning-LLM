from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("dept_id") == 19) & (F.col("salary") < 111000))
    .select(
        "full_name",
        F.date_format("hire_date", "MMM yyyy").alias("hire_date_label"),
    )
    .orderBy("hire_date_label", F.col("full_name").desc())
)
