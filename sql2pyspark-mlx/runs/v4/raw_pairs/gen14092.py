from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .select("id", "method", "paid_at", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "method", "paid_at")
    .orderBy(F.col("paid_at").desc())
)
