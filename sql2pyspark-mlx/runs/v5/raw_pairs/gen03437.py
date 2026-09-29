from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("shipping_fee").desc(), "id")
w2 = Window.partitionBy("order_id")
w_ordered = Window.partitionBy("channel").orderBy("days_shipped_date_to_delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("discount_pct", "id", "promo_id")
w5 = Window.partitionBy("promo_code").orderBy("order_id_sum_cost", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("carrier").orderBy("shipped_date", "id", "promo_id").rowsBetween(-2, -1)
w7 = Window.partitionBy("promo_code", "carrier").orderBy("cumulative_days_shipped_date_to_delivered_date", "id", "promo_id")
w8 = Window.partitionBy("carrier").orderBy("discount_pct_bucket", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("carrier").orderBy(F.col("first_known_promo_id").desc(), "id", "promo_id")

filtered = (
    df_shipments
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-07-17"))) & F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
    )
)

with_dims = (
    filtered
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .filter(F.col("b.shipping_fee").between(19, 20))
    .select("a.*", F.col("b.shipping_fee"), F.col("b.channel"))
)

scored = (
    with_dims
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "days_shipped_date_to_delivered_date",
        "channel",
        F.ntile(3).over(w).alias("shipping_fee_bucket"),
        F.sum("cost").over(w2).alias("order_id_sum_cost"),
        F.sum("days_shipped_date_to_delivered_date").over(w_ordered).alias("cumulative_days_shipped_date_to_delivered_date"),
    )
)

with_window = (
    scored
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.shipped_date") >= F.col("pr.starts_on")) & (F.col("s.shipped_date") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

scored_shipments = (
    with_window
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "days_shipped_date_to_delivered_date",
        "shipping_fee_bucket",
        "order_id_sum_cost",
        "promo_id",
        "discount_pct",
        F.ntile(4).over(w4).alias("discount_pct_bucket"),
        F.first("promo_id", ignorenulls=True).over(w5).alias("first_known_promo_id"),
        F.avg("order_id_sum_cost").over(w6).alias("order_id_sum_cost_avg_2_prior"),
        F.ntile(5).over(w7).alias("cumulative_days_shipped_date_to_delivered_date_bucket"),
    )
)

with_windows = (
    scored_shipments
    .select(
        "id",
        "carrier",
        "cost",
        "days_shipped_date_to_delivered_date",
        "shipping_fee_bucket",
        "order_id_sum_cost",
        "promo_id",
        "discount_pct",
        "first_known_promo_id",
        "order_id_sum_cost_avg_2_prior",
        F.last("order_id_sum_cost", ignorenulls=True).over(w8).alias("last_known_order_id_sum_cost"),
        F.row_number().over(w9).alias("seq_no"),
    )
)

valid = (
    with_windows
    .filter(F.col("carrier").isNotNull() & F.col("order_id_sum_cost").between(163, 475))
    .select(
        "*",
        (F.col("order_id_sum_cost_avg_2_prior") + F.col("days_shipped_date_to_delivered_date")).alias("order_id_sum_cost_avg_2_prior_plus_days_shipped_date_to_delivered_date"),
        F.coalesce("seq_no", F.lit(0)).alias("seq_no_filled"),
    )
)

report = (
    valid
    .select(
        "id",
        "carrier",
        "cost",
        "days_shipped_date_to_delivered_date",
        "promo_id",
        "discount_pct",
        "first_known_promo_id",
        "order_id_sum_cost_avg_2_prior",
        "last_known_order_id_sum_cost",
        F.when(F.col("carrier").rlike("^(FedEx|DHL)$"), "CORE").otherwise("PRIORITY").alias("carrier_zone"),
    )
)

result = (
    report
    .select(
        "carrier",
        "cost",
        "promo_id",
        "discount_pct",
        "first_known_promo_id",
        "order_id_sum_cost_avg_2_prior",
        "last_known_order_id_sum_cost",
        F.when(F.col("carrier").rlike("^(UPS|FedEx)$"), "PRIORITY").when(F.col("carrier").rlike("^(DHL|Kerry)$"), "GROUP_B").otherwise("GROUP_A").alias("carrier_zone_2"),
    )
)
