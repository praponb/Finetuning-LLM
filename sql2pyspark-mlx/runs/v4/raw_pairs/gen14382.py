from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("budget").between(3345000, 3569000))
    .select("id", "location", "budget", F.row_number().over(w).alias("rn"))
)
