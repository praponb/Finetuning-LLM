from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .filter(~F.col("status").isin(["REFUNDED", "SUCCESS"]))
    .select("id", "status", "paid_at", F.rank().over(w).alias("paid_at_rank"))
)
