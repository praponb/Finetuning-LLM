from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("days_delivered_date_to_shipped_date").asc_nulls_first())
w_hist = Window.partitionBy("order_id").orderBy(F.col("days_delivered_date_to_shipped_date").asc_nulls_last())
w3 = Window.partitionBy("status").orderBy("shipped_date", "id")

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "shipped_date",
        "status",
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
        F.initcap(F.trim("carrier")).alias("carrier_title"),
    )
)

valid = (
    cleaned
    .filter((F.col("shipped_date") >= F.lit("2023-03-07").cast("date")) & F.col("order_id").isNotNull() & (F.col("order_id") == 5))
    .select("*", F.last_day("shipped_date").alias("shipped_date_month_end"))
)

latest = (
    valid
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    latest
    .withColumn("days_delivered_date_to_shipped_date_percent_rank", F.percent_rank().over(w_hist))
    .withColumn("next_days_delivered_date_to_shipped_date", F.lead("days_delivered_date_to_shipped_date", 1).over(w3))
)

kept_keys = (
    metrics
    .select("order_id", "status")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date").between("2024-10-01", "2025-06-01"))
        .select("order_id", "status")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.channel"))
)

result = (
    with_dims
    .select(
        "id",
        "shipped_date",
        "status",
        "days_delivered_date_to_shipped_date",
        "shipped_date_month_end",
        "dedup_rank",
        "channel",
    )
)
