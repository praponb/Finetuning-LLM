from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())
w2 = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.lag("budget", 2).over(w2).alias("prev_budget"),
    )
)
