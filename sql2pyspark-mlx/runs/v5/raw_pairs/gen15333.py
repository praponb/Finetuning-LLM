from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost", "id")
w2 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("cost") <= 85))
    .withColumn("cost_band", F.when(F.col("cost") >= 88, "High").when(F.col("cost") >= 46, "Medium").otherwise("Low"))
    .withColumn("carrier_clean", F.coalesce("carrier", F.lit("OTHER")))
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.shipping_fee") >= 26)
    .select("e.*", F.col("d.status").alias("order_status"), F.col("d.amount"))
)

enriched_shipments = (
    enriched
    .withColumn("last7_max_cost", F.max("cost").over(w2))
    .withColumn("last7_count_cost", F.count("cost").over(w2))
    .withColumn("last7_min_cost", F.min("cost").over(w2))
)

enriched_shipments_2 = (
    enriched_shipments
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.discount"))
)

result = (
    enriched_shipments_2
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost_band",
        "carrier_clean",
        "order_status",
        "amount",
        "last7_max_cost",
        "last7_min_cost",
    )
    .orderBy(F.col("amount").asc_nulls_last(), F.col("carrier").desc())
)
