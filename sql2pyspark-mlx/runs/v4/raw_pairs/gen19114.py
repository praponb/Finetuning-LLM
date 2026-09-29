from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w2 = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("location").orderBy("budget", "id")
w5 = Window.orderBy("budget", "id")

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_upper", F.upper("dept_name"))
)

result = (
    base
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w)), 4))
    .withColumn("budget_avg_6", F.avg("budget").over(w2))
    .withColumn("running_budget", F.sum("budget").over(w3))
    .withColumn("next_budget", F.lead("budget", 2).over(w4))
    .withColumn("budget_bucket", F.ntile(4).over(w5))
)
