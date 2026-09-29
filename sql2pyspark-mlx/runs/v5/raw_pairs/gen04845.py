from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc_nulls_first(), "id")

base = (
    df_employees
    .filter(F.month("hire_date").isin([3, 8, 11]))
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.upper(F.trim("email")).alias("email_normalized"),
    )
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "manager_id", "job_title", "level")
)
