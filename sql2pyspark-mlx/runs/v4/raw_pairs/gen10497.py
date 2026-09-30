from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc(), "id")
w_part = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").asc_nulls_last())
w3 = Window.orderBy("budget", "id")
w4 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("location").orderBy(F.col("budget_percent_rank").desc(), "id")

deduped = (
    df_departments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    deduped
    .select(
        "*",
        F.percent_rank().over(w_part).alias("budget_percent_rank"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.sum("budget_percent_rank").over(w4).alias("cumulative_budget_percent_rank"),
        F.row_number().over(w5).alias("seq_no_2"),
    )
)

result = (
    enriched
    .groupBy(
        "dept_name",
        F.when(F.col("seq_no_2") >= 7, "LARGE").otherwise("SMALL").alias("seq_no_2_tier"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Bangkok", F.col("seq_no")).otherwise(0)).alias("bangkok_seq_no"),
        F.count_if(F.col("dept_name") == "HR").alias("hr_events"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
    .filter(F.col("bangkok_seq_no") < 0)
)
