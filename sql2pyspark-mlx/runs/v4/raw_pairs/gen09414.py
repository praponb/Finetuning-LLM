from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "dept_id").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("manager_id")
w3 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("salary").desc(), "id")
w5 = Window.partitionBy("manager_id", "level")
w6 = Window.orderBy(F.col("salary").desc())
w7 = Window.partitionBy("level").orderBy("running_salary", "id").rowsBetween(-3, -1)
w8 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_employees
    .withColumn("salary_rank", F.rank().over(w))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w2)), 4))
    .withColumn("running_salary", F.sum("salary").over(w3))
)

metrics = (
    with_windows
    .select(
        "*",
        F.ntile(4).over(w4).alias("salary_bucket"),
        F.avg("salary").over(w5).alias("manager_id_avg_salary"),
        F.cume_dist().over(w6).alias("salary_cume_dist"),
    )
)

scored = (
    metrics
    .withColumn("salary_rank_avg_3_prior", F.avg("salary_rank").over(w7))
    .withColumn("first_known_level", F.first("level", ignorenulls=True).over(w8))
)

result = (
    scored
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
    )
    .orderBy(F.col("salary").desc_nulls_first())
)
