from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("status")
w3 = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_first())

result = (
    df_payments
    .select(
        "*",
        F.sum("amount").over(w).alias("running_amount"),
        F.min("amount").over(w_grp).alias("status_min_amount"),
        F.rank().over(w3).alias("amount_rank"),
    )
)
