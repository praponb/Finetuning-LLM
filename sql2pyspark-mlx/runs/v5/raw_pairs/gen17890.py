from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")

deduped = (
    df_employees
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "full_name", "email", "manager_id", "job_title", "salary")
)

result = (
    deduped
    .withColumn("salary_tier", F.when(F.col("salary") >= 120000, "PREMIUM").otherwise("STANDARD"))
    .cube("manager_id", "job_title", "salary_tier")
    .agg(
        F.min("salary").alias("min_salary"),
        F.sum(F.when(F.col("job_title") == "Analyst", F.col("salary")).otherwise(0)).alias("analyst_salary"),
        F.count_if(F.col("job_title") == "Analyst").alias("analyst_events"),
        F.max(F.when(F.col("job_title") == "Engineer", F.col("salary"))).alias("engineer_max_salary"),
        F.avg(F.when(F.col("job_title") == "Manager", F.col("salary"))).alias("manager_avg_salary"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("manager_id").alias("grouping_id"),
    )
    .orderBy("engineer_max_salary")
)
