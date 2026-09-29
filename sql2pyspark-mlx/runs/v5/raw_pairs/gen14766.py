from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())

result = (
    df_departments
    .filter(F.col("budget") < 3650000)
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.lag("budget", 1).over(w).alias("prev_budget"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
