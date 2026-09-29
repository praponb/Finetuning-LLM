from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount_whole", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("method").orderBy("paid_at_date", "id")
w3 = Window.partitionBy("method").orderBy("paid_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter((F.col("paid_at") > F.to_date(F.lit("2024-03-21"))) & F.col("order_id").isNotNull() & (F.col("method") == "cash"))
    .select(
        "*",
        F.floor("amount").alias("amount_whole"),
        F.coalesce("method", F.lit("N/A")).alias("method_clean"),
    )
)

base = (
    valid
    .filter(F.col("paid_at") > F.lit("2024-05-07 07:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "amount_whole",
        F.to_date("paid_at").alias("paid_at_date"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.avg("amount_whole").over(w).alias("amount_whole_avg_2"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("method", ignorenulls=True).over(w3).alias("last_known_method"),
    )
)

enriched_payments = (
    enriched
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.channel"), F.col("d.shipping_fee"), F.col("d.discount"))
)

result = (
    enriched_payments
    .groupBy(
        F.when(F.col("shipping_fee") >= 32, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.count("*").alias("event_count"),
        F.max(F.when(F.col("channel") == "partner", F.col("amount"))).alias("partner_max_amount"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.round(F.sum("amount_whole_avg_2") / F.nullif(F.count(F.when(F.col("method") == "bank_transfer", 1)), F.lit(0)), 2).alias("amount_whole_avg_2_per_bank_transfer"),
        F.avg(F.when(F.col("channel") == "web", F.col("shipping_fee"))).alias("web_avg_shipping_fee"),
    )
    .orderBy(F.col("amount_whole_avg_2_per_bank_transfer").desc())
)
