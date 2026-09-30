from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy(F.col("discount").desc_nulls_last())

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-06-27").cast("date")) & F.col("carrier").isNotNull())
    .select("*", F.concat("carrier", F.lit(", "), "status").alias("carrier_status"))
)

enriched = (
    cleaned
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.discount"))
)

latest = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

stats = (
    latest
    .groupBy(
        F.when(F.col("cost") >= 94, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("carrier") == "DHL", 1)), F.lit(0)), 2).alias("discount_per_dhl"),
    )
)

result = (
    stats
    .select("cost_tier", "avg_cost", "discount_per_dhl")
    .orderBy(F.col("cost_tier").desc_nulls_last())
    .limit(5)
)
