from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("discount").desc())
w2 = Window.partitionBy("channel").orderBy(F.col("amount").desc_nulls_first(), "id")

latest = (
    df_orders
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

deduped = (
    latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_id", "status", "amount", "shipping_fee", "rn")
)

base = (
    deduped
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "store_id",
        "status",
        "amount",
        "shipping_fee",
        "rn",
        F.round((F.col("shipping_fee") * 1.1), 2).alias("adjusted_shipping_fee"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.product_id") == 1), "left_anti")
)

with_dims = (
    not_flagged
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.opened_date") < "2025-03-01")
    .select("e.*", F.col("b.region"), F.col("b.opened_date"), F.col("b.store_name"))
)

matched_keys = (
    with_dims
    .select("store_id")
    .exceptAll(
        df_orders
        .filter(F.col("discount").between(0.2, 0.28))
        .select("store_id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

with_dims_orders = (
    reconciled
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "inner")
    .select(
        "a.*",
        F.col("p.opened_date").alias("store_opened_date"),
        F.col("p.sqft"),
        F.col("p.store_name").alias("store_store_name"),
    )
)

result = (
    with_dims_orders
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
        "status",
        F.when(F.col("shipping_fee") >= 40, "HIGH").otherwise("LOW").alias("shipping_fee_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.size(F.collect_set("region")).alias("num_regions"),
    )
)
