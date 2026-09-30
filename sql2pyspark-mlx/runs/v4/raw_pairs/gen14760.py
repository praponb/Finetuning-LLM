from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.first("location").over(w).alias("first_location"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
