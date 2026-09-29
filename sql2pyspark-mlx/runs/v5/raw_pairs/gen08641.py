from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("location").orderBy("budget", "id")

enriched = (
    df_departments
    .withColumn("next_budget", F.lead("budget").over(w_recent))
    .withColumn("row_seq", F.row_number().over(w_recent))
)

result = (
    enriched
    .select("id", "dept_name", "location", "budget")
    .orderBy("budget", "location")
)
