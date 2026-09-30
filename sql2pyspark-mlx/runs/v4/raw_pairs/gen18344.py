from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_last())
w3 = Window.orderBy("budget_rounded", "id").rowsBetween(-5, -1)

cleaned = (
    df_departments
    .filter(F.col("location") == "Berlin")
    .withColumn("budget_rounded", F.round("budget", 1))
    .withColumn("location_clean", F.coalesce("location", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .select(
        "*",
        F.sum("budget").over(w).alias("running_budget"),
        F.cume_dist().over(w2).alias("budget_cume_dist"),
        F.sum("budget_rounded").over(w).alias("running_budget_rounded"),
        F.avg("budget").over(w3).alias("budget_avg_5_prior"),
    )
)
