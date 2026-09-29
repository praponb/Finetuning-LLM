from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc_nulls_first(), "id")
w2 = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier", "status")
w4 = Window.orderBy("cost", "id")
w5 = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w6 = Window.partitionBy("carrier", "status").orderBy(F.col("next_cost").desc(), "id")
w7 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("carrier")
w9 = Window.partitionBy("status", "order_id").orderBy(F.col("shipped_date").desc_nulls_last())
amount_by_order_id = df_payments.groupBy("order_id").agg(F.min("amount").alias("min_payment_amount"))

deduped = (
    df_shipments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    deduped
    .select(
        "*",
        F.sum("cost").over(w2).alias("cumulative_cost"),
        F.min("cost").over(w3).alias("carrier_min_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w3)), 4).alias("cost_share"),
        F.row_number().over(w4).alias("row_seq"),
        F.lead("cost", 1).over(w5).alias("next_cost"),
    )
)

enriched = (
    scored
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.discount"), F.col("p.amount"))
)

enriched_shipments = (
    enriched
    .select(
        "*",
        F.row_number().over(w6).alias("event_seq"),
        F.first("cumulative_cost", ignorenulls=True).over(w7).alias("first_known_cumulative_cost"),
        F.round((F.col("cumulative_cost") / F.sum("cumulative_cost").over(w8)), 4).alias("cumulative_cost_share"),
    )
)

ranked = (
    enriched_shipments
    .withColumn("latest_rank", F.rank().over(w9))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

filtered = (
    ranked
    .filter((F.col("delivered_date") >= F.lit("2023-08-24").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("first_known_cumulative_cost_whole", F.floor("first_known_cumulative_cost"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("N/A")))
)

result = (
    filtered
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "status",
        "cumulative_cost",
        "carrier_min_cost",
        "next_cost",
        "event_seq",
        F.when(F.col("carrier").rlike("^(DHL|Kerry)$"), "CORE").otherwise("EXTENDED").alias("carrier_zone"),
        F.col("min_payment_amount"),
    )
)
