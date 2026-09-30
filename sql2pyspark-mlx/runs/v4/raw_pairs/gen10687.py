from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary").desc(), "id")
w_recent = Window.partitionBy("manager_id").orderBy("salary", "id")
w3 = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("manager_id")
w5 = Window.orderBy("salary_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("dept_id").orderBy(F.col("employee_salary").desc(), "id")
w7 = Window.partitionBy("job_title").orderBy(F.col("manager_id_max_salary").desc())
w8 = Window.partitionBy("job_title").orderBy(F.col("manager_id_max_salary").asc_nulls_first())

valid = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select("*", F.add_months("hire_date", 3).alias("hire_date_plus_months"))
)

enriched = (
    valid
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.dept_name"))
)

joined = (
    enriched
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.salary").alias("employee_salary"))
)

with_windows = (
    joined
    .withColumn("salary_bucket", F.ntile(3).over(w))
    .withColumn("salary_bucket_2", F.ntile(5).over(w_recent))
)

metrics = (
    with_windows
    .withColumn("last_known_level", F.last("level", ignorenulls=True).over(w3))
    .withColumn("manager_id_max_salary", F.max("salary").over(w4))
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w5))
)

metrics_employees = (
    metrics
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("manager_id_max_salary_cume_dist", F.cume_dist().over(w7))
)

deduped = (
    metrics_employees
    .withColumn("rank_in_group", F.rank().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

wide = (
    deduped
    .groupBy("dept_id")
    .pivot("dept_name", ["Marketing", "Sales"])
    .agg(F.max("salary_bucket_2").alias("amount"), F.count(F.lit(1)).alias("n"))
)

result = (
    wide
    .select("dept_id", "Marketing_n")
    .orderBy(F.col("Marketing_n").desc())
)
