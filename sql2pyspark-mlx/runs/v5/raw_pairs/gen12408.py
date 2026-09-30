from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = sessionized.select("id", "txn_time", "is_flagged")
