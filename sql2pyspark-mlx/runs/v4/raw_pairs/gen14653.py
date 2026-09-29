from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

base = (
    df_departments
    .filter(F.col("budget") <= 3561000)
    .select(
        "*",
        F.substring("dept_name", 1, 2).alias("dept_name_prefix"),
        F.coalesce("dept_name", F.lit("N/A")).alias("dept_name_clean"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"), F.col("c.salary"))
)

kept_keys = (
    with_latest
    .select("dept_name", "id")
    .exceptAll(
        df_departments
        .filter(F.col("dept_name").isin(["Engineering", "HR", "Marketing"]))
        .select("dept_name", "id")
    )
)

verified = (
    with_latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("location")
    .pivot("dept_name", ["Engineering", "Finance", "Sales"])
    .agg(F.sum("budget").alias("total"), F.count(F.lit(1)).alias("n"))
)
