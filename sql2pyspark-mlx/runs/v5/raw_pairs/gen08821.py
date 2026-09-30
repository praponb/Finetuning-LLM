from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([5, 23, 38]))
    .select(
        "salary",
        "dept_id",
        "hire_date",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.when(F.col("level") == "L3", 1).otherwise(0).alias("is_l3"),
    )
)
