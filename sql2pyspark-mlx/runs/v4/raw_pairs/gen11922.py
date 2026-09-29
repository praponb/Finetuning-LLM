from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .select("id", "customer_id", "started_at", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .orderBy("rn")
)
