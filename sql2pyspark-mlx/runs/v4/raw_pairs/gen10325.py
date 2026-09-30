from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost")

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("cost") > 58))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.discount"), F.col("d.order_date"), F.col("d.amount"))
)

cleaned = (
    joined
    .filter(F.col("delivered_date") > F.to_date(F.lit("2023-04-21")))
    .withColumn("cost_filled_minus_discount", (F.col("cost_filled") - F.col("discount")))
)

latest = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

valid_shipments = (
    latest
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "status",
        "cost_filled",
        "discount",
        "order_date",
        "amount",
        "cost_filled_minus_discount",
        F.round((F.col("discount") / F.col("amount")), 2).alias("discount_per_amount"),
    )
)

result = (
    valid_shipments
    .withColumn("quarter_start", F.date_trunc("QUARTER", "shipped_date"))
    .withColumn("discount_tier", F.when(F.col("discount") >= 0.35, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["quarter_start", "discount_tier"], ["quarter_start"]],
        "quarter_start",
        "discount_tier",
    )
    .agg(
        F.round(F.sum("discount_per_amount") / F.nullif(F.count(F.when(F.col("status") == "LOST", 1)), F.lit(0)), 2).alias("discount_per_amount_per_lost"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"),
        F.grouping("quarter_start").alias("grouping_id"),
    )
)
