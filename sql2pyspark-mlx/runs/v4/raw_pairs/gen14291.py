from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_departments
    .withColumn("prev_budget", F.lag("budget", 2).over(w))
    .withColumn("budget_bucket", F.ntile(3).over(w2))
    .withColumn("first_known_location", F.first("location", ignorenulls=True).over(w3))
)

result = (
    metrics
    .select(
        "id",
        "dept_name",
        "budget",
        "prev_budget",
        "budget_bucket",
        "first_known_location",
    )
)
