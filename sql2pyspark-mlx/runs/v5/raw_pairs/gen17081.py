from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("level").isin(["L5", "L4"]), "priority").otherwise("other").alias("level_group"),
    )
    .agg(
        F.min("salary").alias("min_salary"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.countDistinct("salary").alias("unique_job_titles"),
    )
    .filter(F.col("unique_job_titles") < 3)
    .orderBy("min_salary")
)
