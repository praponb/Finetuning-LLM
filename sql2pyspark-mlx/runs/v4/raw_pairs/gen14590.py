from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_transactions
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
    )
)

result = (
    scored
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "amount_share",
        "cumulative_amount",
        F.when(F.col("merchant").rlike("^(Grab|Amazon)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("merchant_zone"),
    )
)
