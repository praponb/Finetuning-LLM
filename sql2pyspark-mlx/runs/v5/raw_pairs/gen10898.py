from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "manager_id").orderBy("salary", "id")
w2 = Window.partitionBy("dept_id", "job_title").orderBy("salary", "id")
w3 = Window.partitionBy("dept_id", "job_title").orderBy(F.col("latest_rank").desc(), "id")
w4 = Window.partitionBy("job_title").orderBy("latest_rank", "id")

valid = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-07-21").cast("date"))
    .withColumn("level_group", F.when(F.col("level").isin(["L1", "L5"]), "Priority").otherwise("Other"))
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    latest
    .withColumn("latest_rank_2", F.row_number().over(w2))
    .filter(F.col("latest_rank_2") == 1)
    .drop("latest_rank_2")
)

deduped_employees = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    deduped_employees
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
        F.max("salary").alias("max_salary"),
        F.sum("salary").alias("total_salary"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.sum("salary").alias("total_salary_2"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .orderBy(F.col("total_salary").desc_nulls_first())
)
