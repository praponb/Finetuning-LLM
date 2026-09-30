from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "dept_id").orderBy("hire_date", "id")
w_running = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(-6, Window.currentRow)
w_running_2 = Window.orderBy("salary", "id")
w4 = Window.partitionBy("dept_id").orderBy("last7_avg_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("job_title").orderBy("last7_sum_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("job_title").orderBy(F.col("last_known_last7_count_salary").desc_nulls_first(), "id")

filtered = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.initcap(F.trim("level")).alias("level_title"),
        F.coalesce("job_title", F.lit("UNKNOWN")).alias("resolved_job_title"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    ranked
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "level_title",
        F.avg("salary").over(w_running).alias("last7_avg_salary"),
        F.sum("salary").over(w_running).alias("last7_sum_salary"),
        F.count("salary").over(w_running).alias("last7_count_salary"),
        F.ntile(4).over(w_running_2).alias("salary_bucket"),
    )
)

with_windows = (
    enriched
    .filter(F.col("salary") >= 128000)
    .select(
        "*",
        F.last("last7_count_salary", ignorenulls=True).over(w4).alias("last_known_last7_count_salary"),
        F.last("last7_avg_salary", ignorenulls=True).over(w5).alias("last_known_last7_avg_salary"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "last7_count_salary",
        "salary_bucket",
        "last_known_last7_count_salary",
        F.when(F.col("full_name").rlike("^(Pim Chaiyaporn|  Carol Diaz)$"), "EXTENDED").when(F.col("full_name").rlike("^(Alice Wong|bob smith)$"), "PRIORITY").otherwise("CORE").alias("full_name_zone"),
    )
    .orderBy(F.col("job_title").desc_nulls_first(), "last7_count_salary")
)
