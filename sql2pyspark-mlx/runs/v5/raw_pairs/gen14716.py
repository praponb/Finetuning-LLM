from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("salary_per_l3").desc())

result = (
    df_employees
    .groupBy(F.date_trunc("QUARTER", "hire_date").alias("quarter_start"), "job_title")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count_if(F.col("job_title") == "Engineer").alias("engineer_events"),
        F.max("salary").alias("max_salary"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("level") == "L3", 1)), F.lit(0)), 2).alias("salary_per_l3"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
    )
    .withColumn("salary_per_l3_rank", F.rank().over(w))
)
