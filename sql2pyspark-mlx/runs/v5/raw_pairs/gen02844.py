from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .select("id", "method", "paid_at", F.rank().over(w).alias("paid_at_rank"))
)
