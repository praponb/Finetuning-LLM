from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(11, 17))
    .select(
        "id",
        "txn_type",
        "is_flagged",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
    .orderBy("is_flagged_label", "txn_type", "id")
    .limit(20)
)
