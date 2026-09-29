from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "level").orderBy("hire_date", "id")

ranked = (
    df_employees
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
    )
)

result = (
    ranked
    .groupBy(F.date_trunc("year", "hire_date").alias("year_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("job_title") == "Analyst").alias("analyst_events"),
    )
)
