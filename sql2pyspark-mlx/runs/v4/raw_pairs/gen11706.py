from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("budget_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location", "dept_name").orderBy("budget_whole", "id")
w3 = Window.partitionBy("dept_name").orderBy("budget_whole", "id")
w_part_2 = Window.partitionBy("dept_name", "location").orderBy("budget")

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("budget_whole", F.ceil("budget"))
)

result = (
    cleaned
    .withColumn("running_budget_whole", F.sum("budget_whole").over(w_part))
    .withColumn("next_budget", F.lead("budget", 1).over(w2))
    .withColumn("budget_whole_bucket", F.ntile(3).over(w3))
    .withColumn("budget_cume_dist", F.cume_dist().over(w_part_2))
)
