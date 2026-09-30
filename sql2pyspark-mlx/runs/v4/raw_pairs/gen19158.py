from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("order_id").orderBy("cost")

valid = (
    df_shipments
    .filter(F.col("shipped_date") > F.lit("2023-05-08").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.round("cost", 0).alias("cost_rounded"),
    )
)

filtered = (
    valid
    .filter(F.col("order_id").isin([20, 31]))
    .withColumn("cost_rounded_level", F.when(F.col("cost_rounded") >= 75, "High").when(F.col("cost_rounded") >= 20, "Medium").otherwise("Low"))
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    latest
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
)

with_dims = (
    ranked
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.shipping_fee"), F.col("d.channel"), F.col("d.status"))
)

stats = (
    with_dims
    .groupBy(
        "carrier",
        F.when(F.col("cost") >= 80, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.min("shipping_fee").alias("min_shipping_fee"),
        F.avg("cost_rounded").alias("avg_cost_rounded"),
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("cost_rounded")).otherwise(0)).alias("shipped_cost_rounded"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.sum("cost").alias("total_cost"),
    )
)

result = (
    stats
    .select(
        "carrier",
        "cost_tier",
        "min_shipping_fee",
        "mobile_count",
        "total_cost",
        F.when(F.col("carrier").rlike("^(UPS|DHL)$"), "GROUP_A").when(F.col("carrier").rlike("^(FedEx|Kerry)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("carrier_zone"),
    )
    .orderBy(F.col("carrier").desc_nulls_last())
)
