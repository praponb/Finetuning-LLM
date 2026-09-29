from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy("cost", "id")
w3 = Window.partitionBy("carrier", "status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("shipped_date", "id")
w5 = Window.partitionBy("status", "order_id").orderBy("cost", "id")
w6 = Window.partitionBy("carrier").orderBy(F.col("cost_bucket").desc(), "id")
w7 = Window.partitionBy("carrier", "order_id").orderBy(F.col("prev_cost").asc_nulls_last())
w8 = Window.orderBy(F.col("cost_bucket").asc_nulls_last())
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.min("quantity").alias("min_order_item_quantity"))

filtered = (
    df_shipments
    .filter(F.col("shipped_date") > F.to_date(F.lit("2023-05-01")))
    .select("*", F.initcap(F.trim("status")).alias("status_title"))
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    ranked
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "rank_in_group",
        F.lead("cost", 2).over(w2).alias("next_cost"),
        F.last("carrier", ignorenulls=True).over(w3).alias("last_known_carrier"),
        F.lag("cost", 1, 0).over(w4).alias("prev_cost"),
        F.ntile(10).over(w5).alias("cost_bucket"),
    )
)

enriched_shipments = (
    enriched
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("prev_cost_rank", F.rank().over(w7))
    .withColumn("cost_bucket_rank", F.rank().over(w8))
)

filtered_shipments = (
    enriched_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-02-26").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "*",
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"),
        F.greatest("event_seq", "next_cost").alias("greatest_event_seq_next_cost"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled_2"),
    )
)

enriched_shipments_2 = (
    filtered_shipments
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.discount"))
)

result = (
    enriched_shipments_2
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "rank_in_group",
        "prev_cost",
        "cost_bucket",
        "event_seq",
        "prev_cost_rank",
        F.col("min_order_item_quantity"),
    )
    .orderBy(F.col("rank_in_group").desc_nulls_last(), "id")
)
