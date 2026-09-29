from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")

ranked = (
    df_departments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy("dept_name")
    .agg(
        F.size(F.collect_set("location")).alias("num_locations"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "HR", 1)), F.lit(0)), 2).alias("budget_per_hr"),
        F.sum("budget").alias("total_budget"),
    )
    .orderBy(F.col("num_locations").asc_nulls_last())
)
