from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .select("id", "dept_name", "budget", "location")
    .orderBy(F.col("budget").desc())
)
