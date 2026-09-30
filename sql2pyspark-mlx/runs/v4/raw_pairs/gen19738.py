from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy(F.col("delivered_date").asc_nulls_first(), "id")
w2 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")
w4 = Window.partitionBy("status", "carrier").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status")
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.avg("unit_price").alias("avg_order_item_unit_price"))

filtered = (
    df_shipments
    .filter(F.col("shipped_date") >= F.lit("2023-10-17").cast("date"))
    .withColumn("cost_band", F.when(F.col("cost") >= 86, "High").when(F.col("cost") >= 16, "Medium").otherwise("Low"))
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    deduped
    .select(
        "*",
        F.last("carrier", ignorenulls=True).over(w2).alias("last_known_carrier"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.first("cost", ignorenulls=True).over(w4).alias("first_known_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w5)), 4).alias("cost_share"),
    )
)

enriched = (
    scored
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.*",
        F.col("o.amount"),
        F.col("o.order_date"),
        F.col("o.status").alias("order_status"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.discount"))
)

joined = (
    with_dims
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select(
        "a.*",
        F.col("o.status").alias("order_status_2"),
        F.col("o.order_date").alias("order_order_date"),
        F.col("o.discount").alias("order_discount"),
    )
)

enriched_shipments = (
    joined
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.*", F.col("o.discount").alias("order_discount_2"))
)

result = (
    enriched_shipments
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "status",
        "last_known_carrier",
        "seq_no",
        "first_known_cost",
        "cost_share",
        "order_status",
        F.when(F.col("order_status").rlike("^(RETURNED|SHIPPED)$"), "PRIORITY").otherwise("GROUP_B").alias("order_status_zone"),
        F.col("avg_order_item_unit_price"),
    )
    .orderBy(F.col("order_id").desc(), "first_known_cost")
)
