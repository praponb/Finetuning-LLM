from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.avg("unit_price").alias("avg_order_item_unit_price"))

scored = (
    df_shipments
    .withColumn("running_max_cost", F.max("cost").over(w_hist))
    .withColumn("running_count_cost", F.count("cost").over(w_hist))
    .withColumn("running_avg_cost", F.avg("cost").over(w_hist))
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w_grp))
)

joined = (
    scored
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.delivered_date",
        "e.running_max_cost",
        "e.running_avg_cost",
        "e.first_known_cost",
        F.col("b.channel"),
        F.col("b.shipping_fee"),
    )
)

remaining_keys = (
    joined
    .select("order_id", "id")
    .intersect(
        df_shipments
        .filter(F.col("carrier").isin(["Kerry", "DHL", "FedEx"]))
        .select("order_id", "id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("first_known_cost").isNotNull())
    .select("*", F.ceil("running_avg_cost").alias("running_avg_cost_whole"))
)

result = (
    base
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "delivered_date",
        "running_max_cost",
        "running_avg_cost",
        "first_known_cost",
        "channel",
        "shipping_fee",
        "running_avg_cost_whole",
        F.when(F.col("channel").rlike("^(partner|web)$"), "GROUP_B").otherwise("EXTENDED").alias("channel_zone"),
        F.col("avg_order_item_unit_price"),
    )
)
