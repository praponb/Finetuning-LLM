from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("activity_month").orderBy(F.col("distinct_ids").desc())

cleaned = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.col("salary").cast("int").alias("salary_int"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("month", "hire_date").alias("activity_month"), "level")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
    .withColumn("distinct_ids_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("level").asc_nulls_first())
)
