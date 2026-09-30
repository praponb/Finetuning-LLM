from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy("paid_at", "id")

result = (
    df_payments
    .select("id", "method", "status", "paid_at", F.row_number().over(w).alias("rn"))
    .orderBy(F.col("id").desc())
)
