from pyspark.sql import functions as F

src = (
    df_employees
    .filter(~(F.col("dept_id").isin([22, 27, 30, 35]) & (F.col("job_title").isNull() | (F.col("manager_id") == 10))))
    .withColumn("job_title_group", F.when(F.col("job_title").isin(["Intern", "Designer"]), "Tier 1").otherwise("Other"))
)

agg = (
    src
    .groupBy("level", "job_title")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_count"),
    )
)

result = agg.select("level", "job_title", "stddev_salary", "l4_count")
