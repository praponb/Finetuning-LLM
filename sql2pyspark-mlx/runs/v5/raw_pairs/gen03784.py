from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").asc_nulls_last(), "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget_filled").desc(), "id")

valid = (
    df_departments
    .filter(F.col("location") == "Chiang Mai")
    .select(
        "*",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
        F.coalesce("dept_name", F.lit("N/A")).alias("resolved_dept_name"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "dept_name", "location", "budget", "budget_filled", "dept_name_title")
)

deduped_departments = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

not_flagged = (
    deduped_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.dept_id").isin([5, 8, 37, 40])), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.job_title") == "Manager"), "left_anti")
)

result = (
    eligible
    .select("id", "budget_filled", "dept_name_title")
    .orderBy(F.col("budget_filled").desc_nulls_first())
)
