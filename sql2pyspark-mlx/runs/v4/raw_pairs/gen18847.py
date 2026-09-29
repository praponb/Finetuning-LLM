from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .select(
        "id",
        "status",
        "paid_at",
        F.percent_rank().over(w).alias("paid_at_pct_rank"),
    )
)
