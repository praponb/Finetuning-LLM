from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_salary").desc())

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 147000, "high").when(F.col("salary") >= 50000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count("*").alias("cnt"),
    )
    .withColumn("salary_rank", F.rank().over(w))
    .orderBy(F.col("cnt").desc())
)
