from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("dept_id", "manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("hire_date", "id")
w4 = Window.partitionBy("dept_id")
w5 = Window.partitionBy("dept_id").orderBy("event_seq", "id")

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-09-25"))) & F.col("hire_date").between("2023-11-01", "2025-04-01"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        F.lower("job_title").alias("job_title_lower"),
        F.concat_ws(",", "job_title", "email").alias("job_title_email_key"),
    )
)

metrics = (
    filtered
    .withColumn("salary_avg_5_prior", F.avg("salary").over(w))
    .withColumn("first_known_level", F.first("level", ignorenulls=True).over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("dept_id_min_salary", F.min("salary").over(w4))
)

deduped = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

pivoted = (
    deduped
    .groupBy("manager_id")
    .pivot("level", ["L2", "L1", "L4"])
    .agg(F.sum("salary_avg_5_prior"))
)

result = (
    pivoted
    .select("manager_id", "L2")
    .orderBy(F.col("L2").desc_nulls_first(), "manager_id")
    .limit(5)
)
