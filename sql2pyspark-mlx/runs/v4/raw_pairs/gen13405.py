from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc(), "id")
w3 = Window.partitionBy("status").orderBy(F.col("shipped_date").asc_nulls_first(), "id")
w4 = Window.partitionBy("status").orderBy(F.col("delivered_date").desc_nulls_last())

valid = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2024-03-07").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "*",
        F.trim("carrier").alias("clean_carrier"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"),
    )
)

enriched = (
    valid
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w_recent))
    .withColumn("seq_no", F.row_number().over(w2))
)

valid_shipments = (
    enriched
    .filter((F.col("delivered_date") > F.lit("2023-04-05").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("status_length", F.length("status"))
)

enriched_shipments = (
    valid_shipments
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.amount"), F.col("d.channel"))
)

ranked = (
    enriched_shipments
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

deduped = (
    ranked
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

base = (
    deduped
    .filter(F.col("status_length") <= 26)
    .select(
        "*",
        F.round("status_length", 0).alias("status_length_rounded"),
        F.floor("seq_no").alias("seq_no_whole"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled_2"),
    )
)

result = (
    base
    .groupBy("order_id")
    .pivot("carrier", ["Kerry", "Flash", "FedEx"])
    .agg(F.max("last_known_cost"))
    .select(
        "order_id",
        F.col("Kerry").alias("kerry"),
        F.col("Flash").alias("flash"),
        F.col("FedEx").alias("fedex"),
    )
)
