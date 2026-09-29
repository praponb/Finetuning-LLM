from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy("cost", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("delivered_date", "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

enriched = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.channel"), F.col("b.amount"))
)

scored = (
    enriched
    .withColumn("amount_avg_5", F.avg("amount").over(w))
    .withColumn("cost_avg_6", F.avg("cost").over(w2))
    .withColumn("cost_avg_5", F.avg("cost").over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

with_window = (
    scored
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.delivered_date") >= F.col("pr.starts_on")) & (F.col("s.delivered_date") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    with_window
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "channel",
        "amount",
        "cost_avg_6",
        "promo_id",
        F.col("avg_shipment_cost"),
    )
    .orderBy(F.col("avg_shipment_cost").desc())
)
