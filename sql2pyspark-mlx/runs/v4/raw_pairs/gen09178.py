from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "country").orderBy(F.col("started_at").desc_nulls_first(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("device").orderBy("started_at", "id")
w4 = Window.partitionBy("browser").orderBy("started_at", "id")
w5 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w6 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_sessions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    latest
    .select(
        "*",
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("seq_no_2"),
    )
)

joined = (
    with_windows
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.city"))
)

with_gaps = (
    joined
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w5) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w6))

result = (
    sessions_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        "seq_no_2",
        "is_new_session",
        "island_id",
    )
    .orderBy(F.col("seq_no_2").desc(), "is_new_session")
)
