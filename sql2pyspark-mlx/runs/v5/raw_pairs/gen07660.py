from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier", "order_id").orderBy("cost")
w4 = Window.partitionBy("order_id").orderBy("cost_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("cost_cume_dist", "id")
w6 = Window.partitionBy("order_id").orderBy(F.col("cost_cume_dist").desc(), "id")

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-04-25").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "*",
        F.coalesce("carrier", F.lit("unassigned")).alias("carrier_filled"),
        F.date_format("shipped_date", "yyyy-MM-dd").alias("shipped_date_label"),
    )
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_id", "carrier", "cost")
)

with_windows = (
    latest
    .select(
        "id",
        "order_id",
        F.first("carrier", ignorenulls=True).over(w2).alias("first_known_carrier"),
        F.cume_dist().over(w3).alias("cost_cume_dist"),
    )
)

scored = (
    with_windows
    .filter(F.col("order_id").isin([5, 12, 26, 34]))
    .withColumn("cumulative_cost_cume_dist", F.sum("cost_cume_dist").over(w4))
    .withColumn("next_cost_cume_dist", F.lead("cost_cume_dist", 2).over(w5))
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

kept_keys = (
    deduped
    .select("order_id")
    .exceptAll(
        df_shipments
        .filter(F.col("order_id") == 39)
        .select("order_id")
    )
)

verified = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.*", F.col("p.discount"), F.col("p.order_date"), F.col("p.status"))
)

result = (
    joined
    .select(
        "id",
        "order_id",
        "cost_cume_dist",
        "next_cost_cume_dist",
        "discount",
        "order_date",
        "status",
        F.when(F.col("status").rlike("^(SHIPPED|RETURNED)$"), "OTHER_ZONE").when(F.col("status").rlike("^(COMPLETED|PENDING)$"), "CORE").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(F.col("discount").desc_nulls_last())
)
