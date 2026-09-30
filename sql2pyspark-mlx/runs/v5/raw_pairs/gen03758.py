from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Analyst", "Engineer"]))
    .withColumn("full_name_normalized", F.upper(F.trim("full_name")))
    .withColumn("level_group", F.when(F.col("level").isin(["L2", "L5"]), "Group A").otherwise("Other"))
    .withColumn("resolved_job_title", F.coalesce("job_title", F.lit("NONE")))
)

result = (
    filtered
    .groupBy("dept_id")
    .pivot("job_title", ["Director", "Manager"])
    .agg(F.max("salary").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "dept_id",
        F.col("Director_amount").alias("director_amount"),
        F.col("Director_cnt").alias("director_cnt"),
        F.col("Manager_amount").alias("manager_amount"),
        F.col("Manager_cnt").alias("manager_cnt"),
    )
)
