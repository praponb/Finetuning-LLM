from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("budget", "id")

result = (
    df_departments
    .withColumn("first_known_location", F.first("location", ignorenulls=True).over(w))
    .withColumn("budget_bucket", F.ntile(5).over(w2))
)
