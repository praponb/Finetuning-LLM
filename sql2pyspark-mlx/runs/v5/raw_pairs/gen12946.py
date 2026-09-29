from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")
w2 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy("dept_name", "level").orderBy("salary", "id")
w5 = Window.partitionBy("dept_name").orderBy(F.col("dept_id_avg_salary").desc_nulls_first(), "id")
w6 = Window.partitionBy("dept_id", "job_title")
w7 = Window.partitionBy("location", "manager_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("location", "department_location").orderBy("salary_bucket_2", "id")

with_dims = (
    df_employees
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.budget"), F.col("d.dept_name"), F.col("d.location"))
)

metrics = (
    with_dims
    .filter(F.col("level") == "L1")
    .withColumn("dept_id_avg_salary", F.avg("salary").over(w))
    .withColumn("first_known_location", F.first("location", ignorenulls=True).over(w2))
    .withColumn("salary_bucket", F.ntile(3).over(w_recent))
    .withColumn("salary_bucket_2", F.ntile(4).over(w4))
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    deduped
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.location").alias("department_location"),
        F.col("p.dept_name").alias("department_dept_name"),
    )
)

result = (
    joined
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        "dept_name",
        "location",
        "first_known_location",
        "salary_bucket_2",
        "dedup_rank",
        "department_location",
        "department_dept_name",
        F.round((F.col("salary") / F.sum("salary").over(w6)), 4).alias("salary_share"),
        F.last("department_location", ignorenulls=True).over(w7).alias("last_known_department_location"),
        F.ntile(5).over(w_ordered).alias("salary_bucket_2_bucket"),
    )
)
