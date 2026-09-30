from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Finance", "Legal"]))
    .select("id", "dept_name", "budget", "location", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "dept_name", "budget", "location")
)
