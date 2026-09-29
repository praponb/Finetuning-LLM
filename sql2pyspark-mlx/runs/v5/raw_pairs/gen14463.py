from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w2 = Window.partitionBy("dept_name", "location")
w3 = Window.partitionBy("location", "dept_name").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        F.stddev_samp("budget").over(w).alias("location_stddev_samp_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
        F.lead("budget", 1).over(w3).alias("next_budget"),
    )
)
