from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc())
w2 = Window.partitionBy("status").orderBy("paid_at", "id")
w3 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_payments
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id").eqNullSafe(F.col("o.id")), "left")
    .filter(F.col("o.store_id").isin([7, 14, 27]))
    .select("a.id", "a.order_id", "a.paid_at", "a.status", F.col("o.order_date"))
)

ranked = (
    with_dims
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "paid_at", "status")
)

with_gaps = (
    ranked
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w2) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "paid_at",
        "status",
        F.sum("new_group_flag").over(w3).alias("burst_id"),
    )
)

valid = (
    sessionized
    .filter(F.col("status").isin(["REFUNDED", "SUCCESS"]))
    .select(
        "*",
        F.date_format("paid_at", "yyyy-MM-dd").alias("paid_at_label"),
        F.round("burst_id", 1).alias("burst_id_rounded"),
    )
)

result = (
    valid
    .select("id", "paid_at", "status", "burst_id")
    .orderBy(F.col("burst_id").asc_nulls_last())
)
