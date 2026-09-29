from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("carrier", "order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w_recent = Window.partitionBy("order_id", "carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-10-15"))) & F.col("carrier").isin(["Flash", "UPS", "DHL"]))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        F.date_trunc("WEEK", "shipped_date").alias("shipped_date_period"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

enriched = (
    cleaned
    .withColumn("first_known_carrier", F.first("carrier", ignorenulls=True).over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("last_known_carrier", F.last("carrier", ignorenulls=True).over(w_recent))
)

enriched_shipments = (
    enriched
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.amount"), F.col("b.status"), F.col("b.discount"))
)

cleaned_shipments = (
    enriched_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "shipped_date_period",
        "first_known_carrier",
        "event_seq",
        "amount",
        "status",
        "discount",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
    )
)

result = (
    cleaned_shipments
    .groupBy(
        F.date_trunc("QUARTER", "shipped_date").alias("quarter_start"),
        F.when(F.col("event_seq") >= 8, "HIGH").otherwise("LOW").alias("event_seq_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("event_seq")).otherwise(0)).alias("cancelled_event_seq"),
    )
)
