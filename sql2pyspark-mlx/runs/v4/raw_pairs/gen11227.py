from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget_whole").desc_nulls_last(), "id")
w2 = Window.partitionBy("location", "dept_name")
w_part = Window.partitionBy("location").orderBy(F.col("budget_whole").desc_nulls_last())
w4 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc(), "id")

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("location") != "Chiang Mai"))
    .select(
        "*",
        F.when(F.col("budget") >= 3638000, "High").when(F.col("budget") >= 626000, "Medium").otherwise("Low").alias("budget_tier"),
    )
)

base = (
    valid
    .filter(F.col("dept_name").isNotNull() & (F.col("location") == "Bangkok"))
    .withColumn("budget_whole", F.ceil("budget"))
    .withColumn("dept_name_lower", F.lower("dept_name"))
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

not_flagged = (
    latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.job_title") != "Director"), "left_anti")
)

with_windows = (
    not_flagged
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
    .withColumn("budget_whole_cume_dist", F.cume_dist().over(w_part))
    .withColumn("budget_bucket", F.ntile(10).over(w4))
)

result = (
    with_windows
    .groupBy("dept_name")
    .agg(
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_rows"),
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_rows"),
    )
    .orderBy(F.col("tokyo_rows").desc())
)
