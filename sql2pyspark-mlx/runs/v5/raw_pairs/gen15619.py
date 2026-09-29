from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level")
w4 = Window.orderBy(F.col("salary").desc(), "id")
w5 = Window.partitionBy("manager_id").orderBy("salary_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("level").orderBy(F.col("first_known_level_stddev_samp_salary").desc_nulls_last(), "id")
w_running = Window.partitionBy("level").orderBy("salary_bucket", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("manager_id")
w9 = Window.partitionBy("level").orderBy("last4_min_salary_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_employees
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    deduped
    .select(
        "id",
        "full_name",
        "manager_id",
        "hire_date",
        "is_remote",
        "level",
        F.first("job_title", ignorenulls=True).over(w2).alias("first_known_job_title"),
        F.stddev_samp("salary").over(w3).alias("level_stddev_samp_salary"),
        F.ntile(10).over(w4).alias("salary_bucket"),
    )
)

metrics = (
    enriched
    .withColumn("level_avg_level_stddev_samp_salary", F.avg("level_stddev_samp_salary").over(w3))
    .withColumn("first_known_level_stddev_samp_salary", F.first("level_stddev_samp_salary", ignorenulls=True).over(w5))
)

deduped_employees = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped_employees
    .withColumn("last4_avg_salary_bucket", F.avg("salary_bucket").over(w_running))
    .withColumn("last4_min_salary_bucket", F.min("salary_bucket").over(w_running))
    .withColumn("last4_max_salary_bucket", F.max("salary_bucket").over(w_running))
    .withColumn("level_sum_level_avg_level_stddev_samp_salary", F.sum("level_avg_level_stddev_samp_salary").over(w3))
)

enriched_employees = (
    scored
    .filter(F.col("full_name").like("b%"))
    .select(
        "id",
        "full_name",
        "hire_date",
        "level",
        "first_known_job_title",
        "level_stddev_samp_salary",
        "salary_bucket",
        "first_known_level_stddev_samp_salary",
        "level_sum_level_avg_level_stddev_samp_salary",
        F.min("level_avg_level_stddev_samp_salary").over(w8).alias("manager_id_min_level_avg_level_stddev_samp_salary"),
        F.first("level", ignorenulls=True).over(w9).alias("first_known_level"),
        F.round((F.col("level_sum_level_avg_level_stddev_samp_salary") / F.sum("level_sum_level_avg_level_stddev_samp_salary").over(w8)), 4).alias("level_sum_level_avg_level_stddev_samp_salary_share"),
    )
)

result = (
    enriched_employees
    .select(
        "id",
        "full_name",
        "first_known_job_title",
        "level_stddev_samp_salary",
        "first_known_level_stddev_samp_salary",
        "manager_id_min_level_avg_level_stddev_samp_salary",
        "first_known_level",
        "level_sum_level_avg_level_stddev_samp_salary_share",
        F.when(F.col("full_name").rlike("^(Ivy Chen|Alice Wong)$"), "CORE").when(F.col("full_name").rlike("^(bob smith|  Carol Diaz)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("full_name_zone"),
    )
    .orderBy(F.col("id").desc(), "level_stddev_samp_salary")
)
