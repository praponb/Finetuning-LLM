from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.partitionBy("dept_name", "location")

base = (
    df_departments
    .filter(F.col("budget") <= 2389000)
    .select("*", F.initcap(F.trim("dept_name")).alias("dept_name_title"))
)

result = (
    base
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w)), 4))
    .withColumn("budget_share_2", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
)
