from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Finance", "Legal"]))
    .select("id", "budget", F.lead("budget", 1).over(w).alias("next_budget"))
)
