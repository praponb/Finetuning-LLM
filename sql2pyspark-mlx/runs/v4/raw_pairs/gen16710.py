from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")

latest = (
    df_employees
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .groupingSets([["dept_id", "level"], ["dept_id"]], "dept_id", "level")
    .agg(
        F.count_if(F.col("job_title") == "Manager").alias("manager_events"),
        F.count_if(F.col("job_title") == "Manager").alias("manager_events_2"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.grouping("dept_id").alias("grouping_level"),
    )
    .orderBy(F.col("level").asc_nulls_last())
)
