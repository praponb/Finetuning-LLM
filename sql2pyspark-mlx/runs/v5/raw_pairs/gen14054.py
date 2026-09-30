from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("order_id", "carrier").orderBy("cost", "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

filtered = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-06-21").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("days_since_shipped_date", F.datediff(F.current_date(), "shipped_date"))
    .withColumn("carrier_clean", F.coalesce("carrier", F.lit("N/A")))
)

with_windows = (
    filtered
    .filter(F.col("carrier") != "Flash")
    .withColumn("next_days_since_shipped_date", F.lead("days_since_shipped_date").over(w_hist))
    .withColumn("prev_days_since_shipped_date", F.lag("days_since_shipped_date").over(w_hist))
    .withColumn("next_cost", F.lead("cost").over(w_hist))
    .withColumn("row_seq", F.row_number().over(w_hist))
)

valid = (
    with_windows
    .filter(~F.col("carrier").eqNullSafe(F.lit("Kerry")))
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "status",
        "days_since_shipped_date",
        "prev_days_since_shipped_date",
        "next_cost",
        "row_seq",
        F.coalesce("prev_days_since_shipped_date", F.lit(0)).alias("prev_days_since_shipped_date_filled"),
    )
)

joined = (
    valid
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.shipping_fee"))
)

enriched = (
    joined
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.discount"))
)

result = (
    enriched
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "status",
        "days_since_shipped_date",
        "prev_days_since_shipped_date",
        "row_seq",
        "shipping_fee",
        F.col("sum_shipment_cost"),
    )
)
