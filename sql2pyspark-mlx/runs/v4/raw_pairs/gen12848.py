from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("amount_whole").desc())
w2 = Window.orderBy(F.col("amount_whole").desc(), "id")

filtered = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-04-21").cast("date")) & F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        F.ceil("amount").alias("amount_whole"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

with_windows = (
    filtered
    .withColumn("amount_whole_rank", F.rank().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)

joined = (
    with_windows
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.opened_date"), F.col("p2.region"))
)

result = (
    joined
    .groupBy("store_id")
    .agg(
        F.percentile_approx("amount_whole_rank", [0.25, 0.5, 0.75]).alias("amount_whole_rank_quartiles"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("event_seq")).otherwise(0)).alias("pending_event_seq"),
        F.round(F.sum("amount_whole") / F.nullif(F.count(F.when(F.col("status") == "COMPLETED", 1)), F.lit(0)), 2).alias("amount_whole_per_completed"),
    )
)
