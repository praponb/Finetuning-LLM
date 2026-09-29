from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "is_flagged",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 2)
    .orderBy("amount")
)
