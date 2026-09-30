from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location", "dept_name")
w3 = Window.partitionBy("dept_name", "employee_job_title").orderBy("dept_name_length", "id")
w4 = Window.partitionBy("location", "employee_job_title").orderBy(F.col("budget").asc_nulls_first())
w5 = Window.partitionBy("employee_job_title").orderBy(F.col("dept_name_length").desc(), "id")

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select("*", F.length("dept_name").alias("dept_name_length"))
)

with_last = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title").alias("employee_job_title"))
)

scored = (
    with_last
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("budget_rank", F.rank().over(w4))
    .withColumn("dept_name_length_bucket", F.ntile(3).over(w5))
)

stats = (
    scored
    .groupBy(
        F.when(F.col("budget_share") >= 0.69, "HIGH").otherwise("LOW").alias("budget_share_tier"),
    )
    .agg(
        F.count_if(F.col("dept_name") == "Legal").alias("legal_events"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_rows"),
        F.round(F.sum("dept_name_length_bucket") / F.nullif(F.count(F.when(F.col("dept_name") == "Finance", 1)), F.lit(0)), 2).alias("dept_name_length_bucket_per_finance"),
    )
)

result = (
    stats
    .select(
        "budget_share_tier",
        "legal_events",
        "finance_rows",
        "dept_name_length_bucket_per_finance",
    )
    .orderBy(F.col("legal_events").desc())
)
