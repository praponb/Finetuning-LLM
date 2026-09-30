from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").asc_nulls_last())
w2 = Window.partitionBy("carrier")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

base = (
    df_shipments
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-03-05"))) & (F.col("delivered_date") >= "2024-01-23"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
        F.coalesce("carrier", F.lit("N/A")).alias("resolved_carrier"),
    )
)

matched_keys = (
    base
    .select("status", "order_id")
    .subtract(
        df_shipments
        .filter(F.col("status") != "RETURNED")
        .select("status", "order_id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.rank().over(w).alias("cost_rank"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

filtered = (
    metrics
    .filter((F.col("delivered_date") > F.lit("2023-07-13").cast("date")) & F.col("carrier").isNotNull())
    .select("*", F.substring("carrier", 1, 4).alias("carrier_prefix"))
)

matched_keys_2 = (
    filtered
    .select("status", "order_id")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date").between("2024-02-19", "2025-03-01"))
        .select("status", "order_id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "status",
        "carrier_title",
        "cost_rank",
        "cost_share",
        F.col("sum_order_item_quantity"),
    )
    .orderBy(F.col("id").asc_nulls_first())
)
