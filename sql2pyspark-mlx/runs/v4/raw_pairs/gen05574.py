from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at")

result = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "paid_at",
        F.percent_rank().over(w).alias("paid_at_pct_rank"),
    )
    .orderBy("id")
)
