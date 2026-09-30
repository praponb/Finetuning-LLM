from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget_int", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy("budget_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("budget_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy("budget_filled", "id")
w5 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.col("budget").cast("int").alias("budget_int"),
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
    )
)

result = (
    cleaned
    .withColumn("budget_int_avg_2", F.avg("budget_int").over(w))
    .withColumn("cumulative_budget", F.sum("budget").over(w2))
    .withColumn("first_known_budget_int", F.first("budget_int", ignorenulls=True).over(w3))
    .withColumn("prev_budget_int", F.lag("budget_int", 2, 0).over(w4))
    .withColumn("budget_bucket", F.ntile(3).over(w5))
)
