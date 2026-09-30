from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id").rowsBetween(-6, -1)
w2 = Window.orderBy("budget", "id")
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w4 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("location").orderBy("budget", "id")
w6 = Window.partitionBy("dept_name").orderBy(F.col("cumulative_budget").desc_nulls_last(), "id")
w7 = Window.partitionBy("dept_name").orderBy("cumulative_budget", "id")

enriched = (
    df_departments
    .select(
        "*",
        F.avg("budget").over(w).alias("budget_avg_6_prior"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("row_seq_2"),
    )
)

enriched_departments = (
    enriched
    .select(
        "id",
        "dept_name",
        F.row_number().over(w3).alias("row_seq_3"),
        F.sum("budget").over(w4).alias("cumulative_budget"),
        F.ntile(4).over(w5).alias("budget_bucket"),
    )
)

deduped = (
    enriched_departments
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("id", "dept_name", "row_seq_3", "budget_bucket")
)

result = (
    latest
    .select("id", "dept_name", "row_seq_3")
    .orderBy(F.col("id").desc_nulls_first(), "dept_name")
    .limit(10)
)
