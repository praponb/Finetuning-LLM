from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipping_fee").asc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc_nulls_last(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc(), "id")

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") > F.to_date(F.lit("2023-11-27"))) & F.col("order_id").isNotNull() & (F.col("cost") <= 37))
    .select("*", F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"))
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select(
        "src.*",
        F.col("d.status").alias("order_status"),
        F.col("d.shipping_fee"),
        F.col("d.channel"),
    )
)

latest = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

latest_shipments = (
    ranked
    .withColumn("rn_2", F.row_number().over(w3))
    .filter(F.col("rn_2") == 1)
)

result = (
    latest_shipments
    .select(
        "id",
        "order_status",
        F.col("cost").alias("COST"),
        F.col("shipping_fee").alias("SHIPPING_FEE_VAL"),
    )
    .unpivot(["id", "order_status"], ["COST", "SHIPPING_FEE_VAL"], "kpi", "value")
    .filter(F.col("value").isNotNull())
)
