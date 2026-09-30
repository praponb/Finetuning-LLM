from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_last())
w_ordered = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_first())

result = (
    df_departments
    .withColumn("budget_dense_rank", F.dense_rank().over(w))
    .withColumn("cumulative_budget", F.sum("budget").over(w_ordered))
    .withColumn("budget_rank", F.rank().over(w_hist))
)
