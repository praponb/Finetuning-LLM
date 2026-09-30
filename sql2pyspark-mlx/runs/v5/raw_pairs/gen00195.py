from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("dept_name") == "Finance")
    .select("id", "location", "budget", F.ntile(2).over(w).alias("budget_ntile"))
)
