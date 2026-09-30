from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w3 = Window.partitionBy("manager_id")
w4 = Window.partitionBy("job_title").orderBy(F.col("salary_cume_dist").asc_nulls_last())
w5 = Window.partitionBy("job_title", "level").orderBy("salary_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("level").orderBy("salary_share", "id")
w7 = Window.partitionBy("level").orderBy(F.col("salary_cume_dist").desc_nulls_last(), "id")

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-02-19").cast("date")) & (~F.col("level").eqNullSafe(F.lit("L2"))))
    .withColumn("level_lower", F.lower("level"))
)

enriched = (
    filtered
    .filter(F.col("level") == "L4")
    .select(
        "id",
        "job_title",
        "is_remote",
        "level",
        F.sum("salary").over(w_running).alias("running_salary"),
        F.cume_dist().over(w_hist).alias("salary_cume_dist"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
)

enriched_employees = (
    deduped
    .withColumn("running_salary_cume_dist", F.sum("salary_cume_dist").over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
)

deduped_employees = (
    enriched_employees
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "level",
        "running_salary",
        "salary_cume_dist",
        "salary_share",
        "latest_rank",
        "running_salary_cume_dist",
    )
)

result = (
    deduped_employees
    .groupBy(
        F.when(F.col("running_salary") >= 659662, "HIGH").otherwise("LOW").alias("running_salary_tier"),
    )
    .agg(
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_count"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.round(F.avg("salary_cume_dist"), 2).alias("avg_salary_cume_dist"),
        F.count_if(F.col("level") == "L5").alias("l5_events"),
        F.max(F.when(F.col("level") == "L2", F.col("running_salary"))).alias("l2_max_running_salary"),
    )
    .orderBy(F.col("l4_count").desc())
)
