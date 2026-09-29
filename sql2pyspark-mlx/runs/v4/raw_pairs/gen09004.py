from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("status").orderBy("days_delivered_date_to_shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("carrier").orderBy("discount", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy(F.col("discount_avg_5").desc(), "id")
w8 = Window.partitionBy("carrier")
w9 = Window.partitionBy("order_id", "status").orderBy(F.col("cost").desc_nulls_last())
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.min("unit_price").alias("min_order_item_unit_price"))

filtered = (
    df_shipments
    .filter((F.col("delivered_date") > F.to_date(F.lit("2023-08-21"))) & F.col("status").isin(["RETURNED", "LOST"]))
    .withColumn("days_delivered_date_to_shipped_date", F.datediff("shipped_date", "delivered_date"))
    .withColumn("status_carrier", F.concat("status", F.lit(" - "), "carrier"))
)

with_dims = (
    filtered
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.order_date"), F.col("o.shipping_fee"), F.col("o.discount"))
)

scored = (
    with_dims
    .select(
        "*",
        F.avg("discount").over(w).alias("discount_avg_5"),
        F.last("cost", ignorenulls=True).over(w2).alias("last_known_cost"),
        F.sum("discount").over(w_ordered).alias("running_discount"),
        F.sum("discount").over(w4).alias("cumulative_discount"),
    )
)

enriched = (
    scored
    .withColumn("running_discount_avg_6", F.avg("running_discount").over(w_grp))
    .withColumn("cumulative_cumulative_discount", F.sum("cumulative_discount").over(w6))
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

matched_keys = (
    deduped
    .select("status")
    .exceptAll(
        df_shipments
        .filter(F.col("carrier").isin(["UPS", "FedEx", "Kerry"]))
        .select("status")
    )
)

verified = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

scored_shipments = (
    verified
    .select(
        "*",
        F.min("discount").over(w8).alias("carrier_min_discount"),
        F.rank().over(w9).alias("cost_rank"),
    )
)

result = (
    scored_shipments
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "status",
        "days_delivered_date_to_shipped_date",
        "status_carrier",
        "order_date",
        "shipping_fee",
        "last_known_cost",
        "running_discount",
        F.col("min_order_item_unit_price"),
    )
    .orderBy(F.col("running_discount").desc())
)
