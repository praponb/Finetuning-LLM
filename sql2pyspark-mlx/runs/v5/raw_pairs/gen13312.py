from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .select("id", "dept_name", "budget", F.ntile(2).over(w).alias("budget_ntile"))
    .orderBy(F.col("budget").desc(), "budget_ntile")
)
