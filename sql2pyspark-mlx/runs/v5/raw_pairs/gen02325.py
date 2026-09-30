from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")

result = (
    df_departments
    .filter(F.col("dept_name") == "HR")
    .select(
        "id",
        "location",
        "budget",
        F.min("budget").over(w).alias("location_min_budget"),
    )
    .orderBy("budget", F.col("location_min_budget").desc())
)
