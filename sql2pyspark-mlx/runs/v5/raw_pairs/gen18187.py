from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc_nulls_last(), "id")

ranked = (
    df_departments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    ranked
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 4388000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.count(F.when(F.col("dept_name") == "Engineering", 1)).alias("engineering_rows"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget_2"),
        F.count("*").alias("event_count"),
        F.avg(F.when(F.col("dept_name") == "HR", F.col("budget"))).alias("hr_avg_budget"),
    )
    .orderBy(F.col("engineering_rows").asc_nulls_last())
)
