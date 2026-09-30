from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget")
w2 = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.rank().over(w).alias("rnk"),
        (F.col("budget") - F.lag("budget", 1, 0).over(w2)).alias("budget_change"),
    )
)
