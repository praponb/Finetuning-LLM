from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.partitionBy("location")

base = (
    df_departments
    .filter(F.col("location") == "Bangkok")
    .select("*", F.upper("location").alias("location_upper"))
)

result = (
    base
    .select(
        "id",
        "location",
        "location_upper",
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share_2"),
    )
)
