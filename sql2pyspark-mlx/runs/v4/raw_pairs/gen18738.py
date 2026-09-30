from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant")
w2 = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "amount",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.row_number().over(w2).alias("amount_row"),
    )
)
