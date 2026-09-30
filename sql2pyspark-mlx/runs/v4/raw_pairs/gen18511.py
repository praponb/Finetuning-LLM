from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("dept_name_length").desc(), "id")
w3 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("dept_name_length", F.length("dept_name"))
    .withColumn("location_clean", F.coalesce("location", F.lit("OTHER")))
)

with_latest = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title").alias("employee_job_title"))
)

enriched = (
    with_latest
    .select(
        "*",
        F.ntile(10).over(w2).alias("dept_name_length_bucket"),
        F.last("location", ignorenulls=True).over(w3).alias("last_known_location"),
        F.last("dept_name_length", ignorenulls=True).over(w4).alias("last_known_dept_name_length"),
    )
)

result = (
    enriched
    .select(
        "id",
        "location",
        "budget",
        "location_clean",
        "dept_name_length_bucket",
        "last_known_location",
        "last_known_dept_name_length",
    )
    .orderBy("location", "last_known_dept_name_length")
)
