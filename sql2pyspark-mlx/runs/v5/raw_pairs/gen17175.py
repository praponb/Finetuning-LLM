from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").asc_nulls_first())
w3 = Window.orderBy("budget", "id")

metrics = (
    df_departments
    .select(
        "*",
        F.lag("budget", 2).over(w).alias("prev_budget"),
        F.dense_rank().over(w2).alias("budget_dense_rank"),
        F.ntile(3).over(w3).alias("budget_bucket"),
    )
)

result = (
    metrics
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "prev_budget",
        "budget_dense_rank",
        F.when(F.col("location").rlike("^(London|Berlin)$"), "GROUP_B").otherwise("CORE").alias("location_zone"),
    )
)
