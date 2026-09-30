from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")

result = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "Berlin", "New York"]))
    .select(
        "id",
        "location",
        "budget",
        F.min("budget").over(w).alias("location_min_budget"),
    )
)
