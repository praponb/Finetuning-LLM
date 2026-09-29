from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part_2 = Window.partitionBy("merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "*",
        F.sum("amount").over(w_part).alias("cumulative_amount"),
        F.lag("amount", 1).over(w_part_2).alias("prev_amount"),
    )
)
