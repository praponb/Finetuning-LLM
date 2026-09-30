from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("location")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.lead("budget", 1).over(w).alias("next_budget"),
        F.count("*").over(w2).alias("location_count"),
    )
)
