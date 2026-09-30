from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc_nulls_last())
w2 = Window.partitionBy("status").orderBy("paid_at", "id")
w3 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_ordered = Window.partitionBy("status").orderBy("burst_id", "id")
w6 = Window.partitionBy("status").orderBy("paid_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy(F.col("burst_id").desc(), "id")

deduped = (
    df_payments
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_id", "paid_at", "status")
)

valid = (
    deduped
    .filter(F.col("order_id").isNotNull())
    .select("*", F.to_date("paid_at").alias("paid_at_date"))
)

gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w2) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "paid_at",
        "status",
        "paid_at_date",
        F.sum("new_group_flag").over(w3).alias("burst_id"),
    )
)

with_gaps = (
    islands
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w2) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w3))

metrics = (
    sessions_payments
    .select(
        "*",
        F.max("is_new_burst").over(w4).alias("is_new_burst_max_90d"),
        F.ntile(3).over(w_ordered).alias("burst_id_bucket"),
        F.last("is_new_burst", ignorenulls=True).over(w6).alias("last_known_is_new_burst"),
    )
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .select(
        "id",
        "paid_at",
        "status",
        "burst_id",
        "is_new_burst",
        "is_new_burst_max_90d",
        "burst_id_bucket",
    )
)
