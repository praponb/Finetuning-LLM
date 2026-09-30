from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "txn_time",
        "is_flagged",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") == 1)
)
