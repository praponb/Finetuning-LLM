from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.trim("location").alias("clean_location"),
        F.coalesce("location", F.lit("NONE")).alias("location_clean"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.manager_id").isin([20, 31])), "left_anti")
)

metrics = (
    eligible
    .withColumn("prev_budget", F.lag("budget", 2).over(w_ordered))
    .withColumn("last_known_budget", F.last("budget", ignorenulls=True).over(w2))
    .withColumn("last_known_budget_2", F.last("budget", ignorenulls=True).over(w_grp))
)

result = (
    metrics
    .groupBy("location")
    .pivot("dept_name", ["Engineering", "Marketing"])
    .agg(F.avg("last_known_budget").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
