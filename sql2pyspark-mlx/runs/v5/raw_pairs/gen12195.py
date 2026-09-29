from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w3 = Window.partitionBy("dept_name", "location")

result = (
    df_departments
    .filter(F.col("budget") <= 1829000)
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.avg("budget").over(w).alias("budget_avg_6_prior"),
        F.row_number().over(w2).alias("event_seq"),
        F.max("budget").over(w3).alias("dept_name_max_budget"),
    )
)
