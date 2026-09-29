from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_departments
    .select(
        "id",
        "dept_name",
        F.lag("budget", 2).over(w).alias("prev_budget"),
        F.first("location", ignorenulls=True).over(w2).alias("first_known_location"),
    )
)

result = (
    enriched
    .select("id", "prev_budget")
    .orderBy("id", F.col("prev_budget").desc())
)
