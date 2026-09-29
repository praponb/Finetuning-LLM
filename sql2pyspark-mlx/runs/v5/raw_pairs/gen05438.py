from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("location").orderBy("hire_date", "id")
w4 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("location", "dept_name").orderBy(F.col("hire_date").desc(), "id")
w6 = Window.partitionBy("employee_level").orderBy(F.col("hire_date").desc(), "id")
w7 = Window.partitionBy("location").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("dept_name").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("location").orderBy(F.col("row_seq").desc(), "id")

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.is_remote")), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"))
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        F.col("c.level").alias("employee_level"),
        F.col("c.hire_date"),
    )
)

latest = (
    with_latest
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

not_flagged = (
    latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") < F.col("s.hire_date")), "left_anti")
)

with_windows = (
    not_flagged
    .filter(F.col("location") != "Tokyo")
    .select(
        "id",
        "location",
        "rn",
        F.last("employee_level", ignorenulls=True).over(w4).alias("last_known_employee_level"),
        F.row_number().over(w5).alias("seq_no"),
        F.row_number().over(w6).alias("row_seq"),
        F.last("location", ignorenulls=True).over(w7).alias("last_known_location"),
        F.last("location", ignorenulls=True).over(w8).alias("last_known_location_2"),
    )
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "location", "seq_no", "row_seq", "last_known_location")
)

result = deduped.select("id", "location", "seq_no", "last_known_location")
