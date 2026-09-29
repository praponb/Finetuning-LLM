from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("channel").orderBy("shipped_date", "id")
w_running = Window.partitionBy("order_id").orderBy("amount_rounded", "id")
w3 = Window.partitionBy("order_id").orderBy("next_amount_rounded", "id")

base = (
    df_shipments
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-08-05"))) & F.col("carrier").isNotNull())
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("N/A")))
    .withColumn("status_carrier", F.concat("status", F.lit(" "), "carrier"))
)

enriched = (
    base
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.order_date") > "2024-04-01")
    .select("e.*", F.col("o.order_date"), F.col("o.channel"), F.col("o.amount"))
)

valid = (
    enriched
    .filter(F.col("delivered_date") > F.to_date(F.lit("2023-03-18")))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "carrier_filled",
        "status_carrier",
        "channel",
        F.round("amount", 0).alias("amount_rounded"),
        F.date_sub("delivered_date", 30).alias("delivered_date_minus_30d"),
    )
)

scored = (
    valid
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status_carrier",
        "channel",
        "delivered_date_minus_30d",
        F.lead("amount_rounded").over(w_ordered).alias("next_amount_rounded"),
        F.lag("cost").over(w_ordered).alias("prev_cost"),
        F.lead("cost").over(w_ordered).alias("next_cost"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.ntile(10).over(w_running).alias("amount_rounded_bucket"),
    )
)

ranked = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .groupingSets(
        [["status", "shipment_channel"], ["status"], []],
        F.col("p.status").alias("status"),
        F.col("s.channel").alias("shipment_channel"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.cost")).alias("max_cost"),
    )
    .orderBy("max_cost")
)
