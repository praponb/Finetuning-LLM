from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        F.avg("amount").over(w).alias("running_avg_amount"),
        F.min("amount").over(w).alias("running_min_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
    )
)

result = (
    enriched
    .select(
        "id",
        "amount",
        "running_avg_amount",
        "running_min_amount",
        "running_max_amount",
    )
    .orderBy(F.col("running_avg_amount").desc())
)
