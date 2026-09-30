from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("paid_at", "id")
w4 = Window.partitionBy("channel").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

joined = (
    islands
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.discount"))
)

base = (
    joined
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "session_start_flag",
        F.hour("paid_at").alias("paid_at_hour"),
        F.coalesce("method", F.lit("NONE")).alias("method_clean"),
    )
)

with_dims = (
    base
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.amount").alias("order_amount"), F.col("p.channel"))
)

flagged_2 = (
    with_dims
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w3) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_payments = flagged_2.withColumn("burst_id", F.sum("new_group_flag").over(w4))

result = (
    sessions_payments
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "session_start_flag",
        "method_clean",
        "order_amount",
        F.when(F.col("method").rlike("^(card|promptpay)$"), "EXTENDED").otherwise("GROUP_A").alias("method_zone"),
    )
    .orderBy("id")
)
