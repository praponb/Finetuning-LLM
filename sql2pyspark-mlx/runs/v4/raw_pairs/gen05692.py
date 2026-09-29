from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("dept_name_location", F.concat("dept_name", F.lit("/"), "location"))
    .withColumn("resolved_location", F.coalesce("location", F.lit("N/A")))
)

result = (
    cleaned
    .withColumn("first_known_dept_name", F.first("dept_name", ignorenulls=True).over(w))
    .withColumn("budget_bucket", F.ntile(3).over(w2))
)
