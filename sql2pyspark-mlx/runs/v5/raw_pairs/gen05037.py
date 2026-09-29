from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("delivered_date")

base = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-04-06").cast("date")) & F.col("status").isin(["RETURNED", "LOST"]))
    .select(
        "*",
        F.add_months("shipped_date", 3).alias("shipped_date_plus_months"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    ranked
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id").eqNullSafe(F.col("o.id")), "inner")
    .filter(F.col("o.discount") > 0.25)
    .select("src.*", F.col("o.discount"), F.col("o.shipping_fee"))
)

base_shipments = (
    with_dims
    .filter(F.col("shipped_date") >= F.to_date(F.lit("2023-05-05")))
    .select(
        "*",
        F.date_format("shipped_date", "yyyy-MM").alias("shipped_date_label"),
        F.coalesce("carrier", F.lit("N/A")).alias("resolved_carrier_2"),
    )
)

result = (
    base_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "shipped_date_plus_months",
        "latest_rank",
        F.when(F.col("carrier").rlike("^(Kerry|DHL)$"), "CORE").when(F.col("carrier").rlike("^(FedEx|Flash)$"), "GROUP_B").otherwise("EXTENDED").alias("carrier_zone"),
    )
    .orderBy(F.col("id").desc())
)
