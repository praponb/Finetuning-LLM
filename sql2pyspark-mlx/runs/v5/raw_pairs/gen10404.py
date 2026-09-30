from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") > 105000)
    .select(
        "hire_date",
        "is_remote",
        F.ceil("salary").alias("salary_whole"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
    .orderBy("hire_date", F.col("is_remote_flag").desc())
)
