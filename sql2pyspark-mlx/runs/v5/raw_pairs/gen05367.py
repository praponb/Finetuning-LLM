from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").asc_nulls_first())

kept_keys = (
    df_shipments
    .select("status", "order_id")
    .exceptAll(
        df_shipments
        .filter(F.col("cost").between(77, 82))
        .select("status", "order_id")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("delivered_date") > F.lit("2023-04-17").cast("date"))
    .select("*", F.date_add("shipped_date", 72).alias("shipped_date_plus_72d"))
)

enriched = (
    filtered
    .select(
        "*",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.dense_rank().over(w2).alias("cost_dense_rank"),
    )
)

base = (
    enriched
    .filter(F.col("shipped_date") > F.lit("2023-02-27").cast("date"))
    .select(
        "*",
        F.when(F.col("status").isin(["IN_TRANSIT", "RETURNED"]), "Group A").otherwise("Other").alias("status_group"),
    )
)

joined = (
    base
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .filter(F.col("b.channel") != "partner")
    .select("s.*", F.col("b.amount"))
)

filtered_shipments = (
    joined
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-04-24"))) & F.col("order_id").isin([17, 32]))
    .select(
        "*",
        F.lower("carrier").alias("carrier_lower"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("resolved_carrier"),
    )
)

result = (
    filtered_shipments
    .select("carrier", "cost_share", "amount", "cost")
    .unpivot(
        ids=["carrier"],
        values=["cost_share", "amount", "cost"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
