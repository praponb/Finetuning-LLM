from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id").orderBy(F.col("amount").desc_nulls_last())

ranked = (
    df_transactions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "txn_type", "txn_time", "merchant", "is_flagged")
)

result = (
    ranked
    .select(
        "id",
        "txn_type",
        "merchant",
        "is_flagged",
        F.when(F.col("merchant").rlike("^(Lazada|7-Eleven)$"), "PRIORITY").when(F.col("merchant").rlike("^(Grab|Amazon)$"), "CORE").otherwise("OTHER_ZONE").alias("merchant_zone"),
    )
    .orderBy(F.col("merchant_zone").desc())
)
