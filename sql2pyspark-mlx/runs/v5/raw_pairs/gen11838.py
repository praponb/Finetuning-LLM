from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("budget_tier").orderBy(F.col("singapore_total_budget").desc())

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.job_title").isin(["Designer", "Director"])), "left_semi")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("budget") >= 3053000, "LARGE").otherwise("SMALL").alias("budget_tier"),
        "dept_name",
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count_if(F.col("location") == "Chiang Mai").alias("chiang_mai_events"),
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget"))).alias("singapore_total_budget"),
    )
    .filter(F.col("chiang_mai_events") >= 1)
    .withColumn("singapore_total_budget_rank", F.rank().over(w))
    .orderBy(F.col("budget_tier").asc_nulls_first())
)
