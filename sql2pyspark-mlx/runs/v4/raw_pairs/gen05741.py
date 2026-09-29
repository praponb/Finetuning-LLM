from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(
        F.max("salary").alias("max_salary"),
        F.sum("salary").alias("total_salary"),
    )
    .filter(F.col("max_salary") > 132000)
    .select("is_remote", "max_salary", "total_salary")
    .orderBy(F.col("max_salary").desc(), F.col("total_salary").desc(), "is_remote")
    .limit(3)
)
