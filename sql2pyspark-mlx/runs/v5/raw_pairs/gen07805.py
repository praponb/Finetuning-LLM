from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")

base = (
    df_employees
    .filter(F.col("level").isNotNull() & (F.col("level").eqNullSafe(F.lit("L3"))))
    .select(
        "*",
        F.concat("email", F.lit(" "), "job_title").alias("email_job_title"),
        F.coalesce("level", F.lit("N/A")).alias("resolved_level"),
    )
)

result = (
    base
    .select(
        "id",
        "email",
        "salary",
        "is_remote",
        "level",
        "email_job_title",
        F.first("job_title", ignorenulls=True).over(w).alias("first_known_job_title"),
        F.ntile(5).over(w2).alias("salary_bucket"),
    )
)
