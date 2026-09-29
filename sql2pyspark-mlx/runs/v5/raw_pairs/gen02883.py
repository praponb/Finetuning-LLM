from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("director_count").desc())

result = (
    df_employees
    .groupBy(
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
        F.date_trunc("WEEK", "hire_date").alias("week_start"),
        F.when(F.col("salary") >= 146000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
    )
    .agg(
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .withColumn("director_count_rank", F.rank().over(w))
    .orderBy(F.col("salary_tier").asc_nulls_last())
)
