from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("location").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("dept_name") == "Engineering")
    .withColumn("dept_name_group", F.when(F.col("dept_name").isin(["Sales", "Engineering"]), "Tier 1").otherwise("Other"))
)

with_last = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"))
)

enriched = (
    with_last
    .filter(F.col("dept_name").isin(["Finance", "HR"]))
    .withColumn("running_min_salary", F.min("salary").over(w_ordered))
    .withColumn("running_avg_salary", F.avg("salary").over(w_ordered))
    .withColumn("running_count_salary", F.count("salary").over(w_ordered))
)

with_latest = (
    enriched
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"))
)

result = (
    with_latest
    .select(
        "id",
        "location",
        "budget",
        "dept_name_group",
        "salary",
        "running_avg_salary",
        "running_count_salary",
        "hire_date",
        F.when(F.col("location").rlike("^(London|Singapore)$"), "EXTENDED").otherwise("GROUP_A").alias("location_zone"),
    )
)
