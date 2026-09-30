from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")

deduped = (
    df_employees
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("salary") >= 150000, "LARGE").otherwise("SMALL").alias("salary_tier"),
        F.date_trunc("MONTH", "hire_date").alias("activity_month"),
    )
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary_2"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
    .orderBy(F.col("salary_tier").desc_nulls_last())
)
