from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("level").orderBy(F.col("finance_events").desc())

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select("*", F.initcap(F.trim("location")).alias("location_title"))
)

kept = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.year(F.col("c.hire_date")) >= 2025), "left_semi")
)

with_latest = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"), F.col("c.level"))
)

result = (
    with_latest
    .groupBy(
        "level",
        F.when(F.col("salary") >= 152000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Chiang Mai", F.col("budget"))).alias("chiang_mai_total_budget"),
        F.count_if(F.col("dept_name") == "Finance").alias("finance_events"),
    )
    .withColumn("finance_events_rank", F.rank().over(w2))
    .orderBy(F.col("finance_events").desc_nulls_first())
)
