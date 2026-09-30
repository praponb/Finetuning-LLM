from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id")
w2 = Window.orderBy(F.col("salary").desc())

scored = (
    df_employees
    .withColumn("next_salary", F.lead("salary", 1).over(w))
    .withColumn("salary_cume_dist", F.cume_dist().over(w2))
)

result = (
    scored
    .groupBy("job_title")
    .agg(
        F.max("salary").alias("max_salary"),
        F.round(F.sum("salary_cume_dist") / F.nullif(F.count(F.when(F.col("job_title") == "Manager", 1)), F.lit(0)), 2).alias("salary_cume_dist_per_manager"),
        F.round(F.sum("next_salary") / F.nullif(F.count(F.when(F.col("level") == "L2", 1)), F.lit(0)), 2).alias("next_salary_per_l2"),
        F.count_if(F.col("job_title") == "Analyst").alias("analyst_events"),
    )
)
