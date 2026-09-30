from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w_hist = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_name", "job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")
w5 = Window.partitionBy("job_title").orderBy("event_seq", "id")

with_last = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", F.col("c.hire_date"), F.col("c.job_title"))
)

metrics = (
    with_last
    .filter(F.col("job_title") == "Manager")
    .select(
        "*",
        F.last("job_title", ignorenulls=True).over(w_hist).alias("last_known_job_title"),
        F.last("dept_name", ignorenulls=True).over(w_hist).alias("last_known_dept_name"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") < F.col("s.hire_date")), "left_semi")
)

enriched = (
    eligible
    .select(
        "id",
        "hire_date",
        "job_title",
        "last_known_job_title",
        "last_known_dept_name",
        F.last("job_title", ignorenulls=True).over(w3).alias("last_known_job_title_2"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "hire_date",
        "job_title",
        "last_known_job_title",
        "last_known_dept_name",
        "last_known_job_title_2",
        "latest_rank",
        F.when(F.col("job_title").rlike("^(Manager|Director)$"), "OTHER_ZONE").when(F.col("job_title").rlike("^(Engineer|Analyst)$"), "CORE").otherwise("GROUP_B").alias("job_title_zone"),
    )
    .orderBy(F.col("hire_date").desc_nulls_last())
)
