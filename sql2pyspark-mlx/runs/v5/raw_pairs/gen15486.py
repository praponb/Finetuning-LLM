from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")
w2 = Window.orderBy("cost")
w3 = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w_grp = Window.partitionBy("status", "carrier").orderBy("delivered_date", "id")
w7 = Window.partitionBy("status", "carrier")
w_hist = Window.partitionBy("status", "carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("cost_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("order_id")
w11 = Window.partitionBy("status").orderBy("running_cost_share", "id")
w12 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-10-04").cast("date")) & F.col("carrier").isNotNull())
    .withColumn("delivered_date_label", F.date_format("delivered_date", "yyyy-MM"))
    .withColumn("status_carrier", F.concat("status", F.lit(" - "), "carrier"))
)

with_windows = (
    base
    .filter(F.month("delivered_date").isin([1, 8, 12]))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w)), 4))
    .withColumn("cost_cume_dist", F.cume_dist().over(w2))
    .withColumn("running_cost", F.sum("cost").over(w3))
    .withColumn("cost_cume_dist_2", F.cume_dist().over(w2))
    .withColumn("last_known_carrier", F.last("carrier", ignorenulls=True).over(w4))
)

scored = (
    with_windows
    .select(
        "*",
        F.lag("cost_cume_dist", 1).over(w5).alias("prev_cost_cume_dist"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.round((F.col("running_cost") / F.sum("running_cost").over(w7)), 4).alias("running_cost_share"),
        F.last("cost_share", ignorenulls=True).over(w_hist).alias("last_known_cost_share"),
        F.first("cost_cume_dist", ignorenulls=True).over(w9).alias("first_known_cost_cume_dist"),
    )
)

with_windows_shipments = (
    scored
    .select(
        "*",
        F.min("first_known_cost_cume_dist").over(w_ordered).alias("order_id_min_first_known_cost_cume_dist"),
        F.lag("cost_cume_dist", 2).over(w11).alias("prev_cost_cume_dist_2"),
        F.first("prev_cost_cume_dist", ignorenulls=True).over(w12).alias("first_known_prev_cost_cume_dist"),
        F.last("cost_cume_dist", ignorenulls=True).over(w13).alias("last_known_cost_cume_dist"),
    )
)

enriched = (
    with_windows_shipments
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select(
        "a.*",
        F.col("b.amount"),
        F.col("b.channel"),
        F.col("b.status").alias("order_status"),
    )
)

cleaned = (
    enriched
    .filter(F.col("order_id").isNotNull() & F.col("order_id").isin([26, 37]))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        "delivered_date_label",
        "cost_share",
        "running_cost",
        "cost_cume_dist_2",
        "event_seq",
        "running_cost_share",
        "order_id_min_first_known_cost_cume_dist",
        "prev_cost_cume_dist_2",
        "first_known_prev_cost_cume_dist",
        "last_known_cost_cume_dist",
        "amount",
        "channel",
        "order_status",
        (F.col("prev_cost_cume_dist") * F.col("running_cost")).alias("prev_cost_cume_dist_times_running_cost"),
        F.coalesce("order_status", F.lit("OTHER")).alias("resolved_order_status"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("last_known_cost_cume_dist") >= 0.77, "LARGE").otherwise("SMALL").alias("last_known_cost_cume_dist_tier"),
    )
    .agg(
        F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_rows"),
        F.round(F.sum("cost_cume_dist_2") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("cost_cume_dist_2_per_partner"),
        F.max("last_known_cost_cume_dist").alias("max_last_known_cost_cume_dist"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("order_status") == "RETURNED", 1)).alias("returned_rows"),
    )
    .filter(F.col("cost_cume_dist_2_per_partner") < 1000000)
    .orderBy(F.col("max_last_known_cost_cume_dist").desc_nulls_first())
)
