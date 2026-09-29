from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .select(
        "id",
        "txn_time",
        "merchant",
        "is_new_session",
        "burst_id",
        F.when(F.col("merchant").rlike("^(Grab|Starbucks)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("merchant_zone"),
    )
    .orderBy(F.col("is_new_session").desc_nulls_last())
)
