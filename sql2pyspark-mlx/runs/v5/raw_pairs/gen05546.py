from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_payments
    .filter(F.col("amount") < 1510)
    .select(
        "id",
        "paid_at",
        "status",
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
        F.avg("amount").over(w).alias("running_avg_amount"),
    )
)
