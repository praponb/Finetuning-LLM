from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select("id", "budget", F.row_number().over(w).alias("rn"))
)
