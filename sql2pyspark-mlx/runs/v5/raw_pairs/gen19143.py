from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .filter(F.hour("paid_at").between(8, 14))
    .select("id", "status", "paid_at", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .orderBy(F.col("id").desc())
)
