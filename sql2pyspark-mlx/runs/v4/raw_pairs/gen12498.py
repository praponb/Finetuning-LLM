from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "dept_id").orderBy("salary", "id")
w2 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("salary", "id")

scored = (
    df_employees
    .withColumn("salary_bucket", F.ntile(5).over(w))
    .withColumn("last_known_job_title", F.last("job_title", ignorenulls=True).over(w2))
    .withColumn("event_seq", F.row_number().over(w_ordered))
)

result = (
    scored
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("activity_month"))
    .agg(
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("level") == "L5", F.col("salary_bucket")).otherwise(0)).alias("l5_salary_bucket"),
    )
)
