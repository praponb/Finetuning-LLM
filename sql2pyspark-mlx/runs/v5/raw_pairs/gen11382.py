from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc(), "id")
w3 = Window.partitionBy("order_id", "status").orderBy("shipped_date", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("order_id")
w_part = Window.partitionBy("order_id").orderBy(F.col("row_seq").desc(), "id")
w6 = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")

valid = (
    df_shipments
    .filter(F.col("shipped_date") > F.lit("2023-03-26").cast("date"))
    .select(
        "*",
        F.when(F.col("carrier").isin(["FedEx", "Kerry"]), "Tier 1").otherwise("Other").alias("carrier_group"),
    )
)

scored = (
    valid
    .filter(F.col("status") != "IN_TRANSIT")
    .withColumn("first_known_carrier", F.first("carrier", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("cost_avg_6_prior", F.avg("cost").over(w3))
)

valid_shipments = (
    scored
    .filter(F.col("shipped_date") > F.lit("2023-04-04").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "carrier_group",
        "first_known_carrier",
        "row_seq",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
    )
)

enriched = (
    valid_shipments
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.delivered_date",
        "src.cost",
        "src.carrier_group",
        "src.first_known_carrier",
        "src.row_seq",
        "src.shipped_date_month_end",
        F.col("p.order_date"),
        F.col("p.shipping_fee"),
    )
)

result = (
    enriched
    .select(
        "id",
        "delivered_date",
        "carrier_group",
        "first_known_carrier",
        "order_date",
        "shipping_fee",
        F.avg("cost").over(w4).alias("order_id_avg_cost"),
        F.ntile(4).over(w_part).alias("row_seq_bucket"),
        F.ntile(4).over(w6).alias("cost_bucket"),
    )
)
