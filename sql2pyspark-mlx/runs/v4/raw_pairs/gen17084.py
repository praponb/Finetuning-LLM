from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary_bucket")

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 153000, "high").when(F.col("salary") >= 72000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("cumulative_salary", F.sum("total_salary").over(w))
    .orderBy(F.col("cumulative_salary").desc())
)
