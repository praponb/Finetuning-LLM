from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())
w_hist = Window.partitionBy("location").orderBy("budget_adjusted", "id").rowsBetween(-6, Window.currentRow)
w_grp = Window.partitionBy("dept_name").orderBy("budget_rounded", "id")

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_rounded", F.round("budget", 1))
    .withColumn("budget_adjusted", F.round((F.col("budget") * 0.85), 2))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("N/A")))
)

result = (
    filtered
    .withColumn("budget_rank", F.rank().over(w))
    .withColumn("budget_avg_6", F.avg("budget").over(w_hist))
    .withColumn("budget_rounded_bucket", F.ntile(4).over(w_grp))
)
