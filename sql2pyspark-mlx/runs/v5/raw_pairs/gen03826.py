from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote") & (F.col("level") != "L2") & F.col("manager_id").isin([3, 7, 19, 28]))
    .select("dept_id", F.col("salary").cast("int").alias("salary_int"))
)
