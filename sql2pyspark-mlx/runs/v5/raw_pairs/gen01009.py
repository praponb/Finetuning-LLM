from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("order_id").orderBy("shipped_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("shipped_date_period", "id")
w4 = Window.partitionBy("carrier").orderBy("shipped_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy(F.col("event_seq").desc_nulls_last())

valid = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-08-05").cast("date")) & (F.col("delivered_date") <= "2023-05-01"))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        F.when(F.col("carrier").isin(["FedEx", "UPS"]), "Tier 1").otherwise("Other").alias("carrier_group"),
        F.date_trunc("MONTH", "shipped_date").alias("shipped_date_period"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_clean"),
    )
)

with_windows = (
    valid
    .withColumn("last_known_carrier", F.last("carrier", ignorenulls=True).over(w_part))
    .withColumn("first_known_carrier", F.first("carrier", ignorenulls=True).over(w_running))
    .withColumn("last_known_carrier_2", F.last("carrier", ignorenulls=True).over(w_running))
)

metrics = (
    with_windows
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("first_known_carrier_2", F.first("carrier", ignorenulls=True).over(w4))
)

remaining_keys = (
    metrics
    .select("id", "carrier")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date") >= "2024-07-01")
        .select("id", "carrier")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .filter(F.col("p.channel") == "mobile")
    .select(
        "src.order_id",
        "src.carrier",
        "src.first_known_carrier",
        "src.last_known_carrier_2",
        "src.event_seq",
        "src.first_known_carrier_2",
        F.col("p.shipping_fee"),
    )
)

scored = (
    enriched
    .select(
        "order_id",
        "carrier",
        "first_known_carrier",
        "last_known_carrier_2",
        "event_seq",
        "first_known_carrier_2",
        F.cume_dist().over(w5).alias("event_seq_cume_dist"),
    )
)

result = (
    scored
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .groupingSets(
        [["channel", "status"], ["channel"], []],
        F.col("d.channel").alias("channel"),
        F.col("d.status").alias("status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("s.event_seq")).alias("avg_event_seq"),
        F.max(F.col("s.event_seq_cume_dist")).alias("max_event_seq_cume_dist"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("status").desc_nulls_last())
)
