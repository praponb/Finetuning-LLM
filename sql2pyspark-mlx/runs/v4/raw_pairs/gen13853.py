from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name", "location")

result = (
    df_departments
    .withColumn("next_budget", F.lead("budget", 1).over(w))
    .withColumn("dept_name_max_budget", F.max("budget").over(w2))
)
