from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("budget_whole", "id")
w2 = Window.partitionBy("location", "dept_name").orderBy("budget_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location")
w_hist = Window.partitionBy("dept_name").orderBy("budget_whole", "id")
w5 = Window.partitionBy("location").orderBy("budget", "id")
w6 = Window.partitionBy("dept_name").orderBy(F.col("running_budget").desc_nulls_last(), "id")

base = (
    df_departments
    .filter(F.col("location").eqNullSafe(F.lit("Bangkok")))
    .select(
        "*",
        F.lower("location").alias("location_lower"),
        F.ceil("budget").alias("budget_whole"),
        F.coalesce("location", F.lit("NONE")).alias("location_clean"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.row_number().over(w_recent).alias("seq_no"),
        F.sum("budget").over(w2).alias("running_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w3)), 4).alias("budget_share"),
        F.lead("budget", 2).over(w_hist).alias("next_budget"),
        F.lead("budget", 2).over(w5).alias("next_budget_2"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id") == 39), "left_anti")
)

latest = (
    not_flagged
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "dept_name", "budget", "location_lower", "budget_whole", "seq_no")
)

result = (
    latest
    .select("id", "dept_name", "budget", "seq_no")
    .orderBy(F.col("seq_no").desc_nulls_last())
)
