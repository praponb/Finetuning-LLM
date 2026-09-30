from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_salary").desc())

result = (
    df_employees
    .filter(F.col("full_name").like("%FRAN%"))
    .groupBy(
        F.when(F.col("salary") >= 127000, "high").when(F.col("salary") >= 43000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.rank().over(w))
)
