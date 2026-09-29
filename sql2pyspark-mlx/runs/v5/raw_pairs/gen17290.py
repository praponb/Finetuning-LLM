from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("carrier")
w4 = Window.partitionBy("order_id").orderBy("cost_share", "id")
w5 = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")
w6 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("running_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy(F.col("running_cost").desc_nulls_first())
w9 = Window.partitionBy("status").orderBy(F.col("running_cost_rank").desc_nulls_first(), "id")

valid = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & F.col("cost").isNull())
    .select(
        "*",
        F.add_months("shipped_date", 6).alias("shipped_date_plus_months"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

metrics = (
    valid
    .filter(F.col("order_id").isNotNull())
    .withColumn("prev_cost", F.lag("cost", 2, 0).over(w))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w2)), 4))
    .withColumn("carrier_max_cost", F.max("cost").over(w3))
)

enriched = (
    metrics
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id").eqNullSafe(F.col("d.id")), "inner")
    .select("s.*", F.col("d.shipping_fee"), F.col("d.amount"))
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "delivered_date",
        "cost",
        "status",
        "shipped_date_plus_months",
        "resolved_carrier",
        "prev_cost",
        "cost_share",
        "carrier_max_cost",
        "shipping_fee",
    )
)

with_windows = (
    deduped
    .select(
        "id",
        "status",
        "shipped_date_plus_months",
        "cost_share",
        F.row_number().over(w5).alias("row_seq"),
        F.sum("cost").over(w6).alias("running_cost"),
    )
)

metrics_shipments = (
    with_windows
    .select(
        "*",
        F.first("cost_share", ignorenulls=True).over(w7).alias("first_known_cost_share"),
        F.rank().over(w8).alias("running_cost_rank"),
    )
)

latest = (
    metrics_shipments
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .withColumn("year_start", F.date_trunc("year", "shipped_date_plus_months"))
    .withColumn("first_known_cost_share_tier", F.when(F.col("first_known_cost_share") >= 0.83, "PREMIUM").otherwise("STANDARD"))
    .cube("status", "year_start", "first_known_cost_share_tier")
    .agg(
        F.round(F.sum("first_known_cost_share") / F.nullif(F.count(F.when(F.col("status") == "DELIVERED", 1)), F.lit(0)), 2).alias("first_known_cost_share_per_delivered"),
        F.count("*").alias("row_count"),
        F.round(F.avg("cost_share"), 2).alias("avg_cost_share"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("first_known_cost_share_per_delivered").asc_nulls_first())
)
