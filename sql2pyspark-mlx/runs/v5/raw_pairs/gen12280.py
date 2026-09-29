from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("dept_id")
w3 = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("manager_id", "level").orderBy(F.col("hire_date").desc_nulls_last())
w5 = Window.partitionBy("dept_id").orderBy("level_sum_salary")
w6 = Window.partitionBy("dept_id", "level").orderBy("salary_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("dept_id").orderBy("salary_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("level").orderBy(F.col("salary_share").desc(), "id")
w9 = Window.orderBy("salary_share", "id").rowsBetween(-3, Window.currentRow)

with_windows = (
    df_employees
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        F.sum("salary").over(w).alias("level_sum_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.first("job_title", ignorenulls=True).over(w3).alias("first_known_job_title"),
    )
)

ranked = (
    with_windows
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
)

ranked_employees = (
    ranked
    .withColumn("dedup_rank", F.rank().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "full_name", "dept_id", "level", "salary_share")
)

metrics = (
    ranked_employees
    .filter(F.col("full_name").isNotNull())
    .select(
        "*",
        F.sum("salary_share").over(w6).alias("cumulative_salary_share"),
        F.sum("salary_share").over(w7).alias("running_salary_share"),
        F.ntile(3).over(w8).alias("salary_share_bucket"),
        F.avg("salary_share").over(w9).alias("salary_share_avg_3"),
    )
)

kept_keys = (
    metrics
    .select("dept_id", "level")
    .exceptAll(
        df_employees
        .filter(F.col("is_remote"))
        .select("dept_id", "level")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "full_name",
        "salary_share",
        "cumulative_salary_share",
        "running_salary_share",
        "salary_share_bucket",
        "salary_share_avg_3",
        F.when(F.col("full_name").rlike("^(Olivia Garcia|Noah Park)$"), "CORE").when(F.col("full_name").rlike("^(Alice Wong|bob smith)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("full_name_zone"),
    )
    .orderBy("salary_share")
)
