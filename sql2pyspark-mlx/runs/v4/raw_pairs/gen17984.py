from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.partitionBy("location", "dept_name").orderBy("budget", "id")

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("budget_level", F.when(F.col("budget") >= 3301000, "High").when(F.col("budget") >= 1340000, "Medium").otherwise("Low"))
    .withColumn("resolved_location", F.coalesce("location", F.lit("UNKNOWN")))
)

result = (
    base
    .select(
        "*",
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.lag("budget", 1).over(w2).alias("prev_budget"),
    )
)
