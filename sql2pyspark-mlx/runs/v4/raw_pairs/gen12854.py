from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")

result = (
    df_departments
    .filter(F.col("budget") < 2144000)
    .select(
        "id",
        "location",
        "budget",
        F.min("budget").over(w).alias("location_min_budget"),
    )
    .orderBy("location")
)
