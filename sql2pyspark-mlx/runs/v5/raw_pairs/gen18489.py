from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        F.initcap(F.trim("job_title")).alias("job_title_title"),
    )
)

result = (
    cleaned
    .groupBy("manager_id")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") < 1000)
    .orderBy(F.col("manager_id").desc_nulls_last())
)
