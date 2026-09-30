from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_rounded", F.round("budget", 1))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("OTHER")))
)

with_last = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"), F.col("c.salary").alias("employee_salary"))
)

matched_keys = (
    with_last
    .select("id")
    .exceptAll(
        df_departments
        .filter(F.col("dept_name") == "Finance")
        .select("id")
    )
)

kept_rows = (
    with_last
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") < F.col("s.hire_date")), "left_semi")
)

result = (
    not_flagged
    .groupBy("location")
    .pivot("dept_name", ["Marketing", "Legal"])
    .agg(F.sum("employee_salary").alias("amount"))
)
