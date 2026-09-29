from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy(F.col("is_legal").asc_nulls_first())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("dept_name", "job_title").orderBy(F.col("is_legal").asc_nulls_first(), "id")
w5 = Window.partitionBy("dept_name").orderBy(F.col("tokyo_rows").desc())

base = (
    df_departments
    .filter(F.col("budget") <= 2619000)
    .select(
        "*",
        F.when(F.col("dept_name") == "Legal", 1).otherwise(0).alias("is_legal"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("resolved_dept_name"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.sum("is_legal").over(w).alias("running_is_legal"),
        F.percent_rank().over(w2).alias("is_legal_percent_rank"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title"), F.col("c.level").alias("employee_level"))
)

eligible = (
    with_last
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.salary") <= 63000), "left_semi")
)

latest = (
    eligible
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy("dept_name", "employee_level")
    .agg(
        F.round(F.sum("is_legal_percent_rank") / F.nullif(F.count(F.when(F.col("employee_level") == "L2", 1)), F.lit(0)), 2).alias("is_legal_percent_rank_per_l2"),
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_rows"),
        F.count(F.when(F.col("employee_level") == "L4", 1)).alias("l4_rows"),
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_count"),
        F.percentile_approx("is_legal_percent_rank", 0.9).alias("is_legal_percent_rank_p90"),
    )
    .filter(F.col("is_legal_percent_rank_p90") < 0)
    .withColumn("tokyo_rows_rank", F.rank().over(w5))
    .orderBy(F.col("is_legal_percent_rank_p90").desc_nulls_last())
)
