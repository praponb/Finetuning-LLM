from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("location", "dept_name").orderBy(F.col("employee_hire_date").desc_nulls_first(), "id")

with_windows = (
    df_departments
    .withColumn("last_known_location", F.last("location", ignorenulls=True).over(w_part))
    .withColumn("first_known_dept_name", F.first("dept_name", ignorenulls=True).over(w_recent))
    .withColumn("budget_bucket", F.ntile(4).over(w3))
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"))
)

latest = (
    latest_match
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

kept = (
    latest
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.dept_id") == 9), "left_anti")
)

result = (
    kept
    .select("id", "dept_name", "budget", "first_known_dept_name", "budget_bucket")
    .orderBy("id", F.col("dept_name").desc())
    .limit(10)
)
