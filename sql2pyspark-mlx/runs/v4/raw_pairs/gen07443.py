from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date", "id")

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("cost") > 65))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        F.upper("status").alias("status_upper"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("resolved_carrier"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.status").alias("order_status"), F.col("d.order_date"))
)

filtered = (
    with_dims
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-06-16"))) & F.col("carrier").isNotNull() & (F.year("shipped_date") >= 2024))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "status_upper",
        "resolved_carrier",
        "order_status",
        "order_date",
        F.when(F.col("status").isin(["DELIVERED", "IN_TRANSIT"]), "Group A").otherwise("Other").alias("status_group"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

totals = (
    ranked
    .groupBy(
        F.date_trunc("quarter", "delivered_date").alias("quarter_start"),
        "order_status",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("order_status")).alias("order_status_list"),
    )
)

result = (
    totals
    .select(
        "quarter_start",
        "order_status",
        "event_count",
        "order_status_list",
        F.when(F.col("order_status").rlike("^(COMPLETED|SHIPPED)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("order_status_zone"),
    )
)
