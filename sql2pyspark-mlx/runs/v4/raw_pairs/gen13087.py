from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")
w3 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_last())

result = (
    df_departments
    .select(
        "*",
        F.lead("budget", 1).over(w).alias("next_budget"),
        F.ntile(3).over(w2).alias("budget_bucket"),
        F.sum("budget").over(w3).alias("cumulative_budget"),
        F.dense_rank().over(w4).alias("budget_dense_rank"),
    )
)
