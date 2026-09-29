from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_last(), "id")

base = (
    df_departments
    .filter(F.col("dept_name") == "Legal")
    .select(
        "*",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("resolved_dept_name"),
    )
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
