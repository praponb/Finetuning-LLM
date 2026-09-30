from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "amount",
        F.row_number().over(w).alias("row_num"),
        F.lead("amount", 2).over(w).alias("next_amount"),
    )
    .orderBy(F.col("next_amount").desc())
)
