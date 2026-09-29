from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_first())

scored = (
    df_departments
    .select(
        "*",
        F.avg("budget").over(w).alias("budget_avg_3_prior"),
        F.percent_rank().over(w2).alias("budget_percent_rank"),
    )
)

result = (
    scored
    .select("id", "dept_name", "budget", "budget_avg_3_prior", "budget_percent_rank")
)
