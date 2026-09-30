from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.lag("budget", 1).over(w).alias("prev_budget"),
        F.ntile(10).over(w2).alias("bucket"),
    )
    .orderBy(F.col("location").desc())
)
