from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("txn_type", "merchant").orderBy("amount_adjusted", "id")
w2 = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_last())
w3 = Window.orderBy("amount_adjusted", "id")

filtered = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.round((F.col("amount") * 1.15), 2).alias("amount_adjusted"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)

result = (
    filtered
    .withColumn("amount_adjusted_bucket", F.ntile(5).over(w_hist))
    .withColumn("amount_percent_rank", F.percent_rank().over(w2))
    .withColumn("next_amount_adjusted", F.lead("amount_adjusted", 1).over(w3))
)
