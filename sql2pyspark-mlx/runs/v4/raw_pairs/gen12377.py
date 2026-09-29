from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id", "level")
w4 = Window.partitionBy("manager_id")

metrics = (
    df_employees
    .withColumn("salary_bucket", F.ntile(4).over(w))
    .withColumn("last_known_job_title", F.last("job_title", ignorenulls=True).over(w2))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w3)), 4))
    .withColumn("manager_id_max_salary", F.max("salary").over(w4))
    .withColumn("next_salary", F.lead("salary", 1).over(w))
)

result = (
    metrics
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "hire_date",
        "level",
        "salary_bucket",
        "last_known_job_title",
        "manager_id_max_salary",
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
