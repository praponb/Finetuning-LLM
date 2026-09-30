from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location", "level").orderBy("budget")
w3 = Window.partitionBy("location").orderBy(F.col("employee_hire_date").desc_nulls_first(), "id")

filtered = (
    df_departments
    .filter(F.col("budget").between(2184000, 3673000))
    .select(
        "*",
        F.lower("location").alias("location_lower"),
        F.concat_ws(",", "dept_name", "location").alias("dept_name_location_key"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.hire_date").alias("employee_hire_date"))
)

eligible = (
    with_latest
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") <= F.col("s.employee_hire_date")), "left_semi")
)

latest = (
    eligible
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
)

ranked = (
    latest
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

eligible_departments = (
    ranked
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (~F.col("c.email").like("%@startup.io")), "left_semi")
)

result = (
    eligible_departments
    .select("id", "budget", "location_lower", "dept_name_location_key")
)
