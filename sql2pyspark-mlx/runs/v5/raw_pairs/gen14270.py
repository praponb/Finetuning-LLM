from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id")

result = (
    df_subscriptions
    .select("id", "status", "end_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "status", "end_date")
    .orderBy("end_date")
)
