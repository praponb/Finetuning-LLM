from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy("is_flagged_flag", "id")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)

result = (
    valid
    .withColumn("is_flagged_flag_avg_2", F.avg("is_flagged_flag").over(w))
    .withColumn("is_flagged_flag_bucket", F.ntile(3).over(w2))
)
