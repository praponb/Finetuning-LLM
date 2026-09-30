from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("department_budget", "id")
w2 = Window.partitionBy("location").orderBy(F.col("salary").desc())

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-04-09").cast("date")) & F.col("level").isNotNull())
    .withColumn("level_lower", F.lower("level"))
)

enriched = (
    valid
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.dept_id",
        "a.manager_id",
        "a.salary",
        "a.hire_date",
        F.col("p.dept_name"),
        F.col("p.budget"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.location"), F.col("d.budget").alias("department_budget"))
)

latest = (
    with_dims
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

deduped = (
    latest
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy("location", "dept_name")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_rows"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
    .filter(F.col("distinct_dept_names") > 1)
)
