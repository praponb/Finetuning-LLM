from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w2 = Window.partitionBy("order_status", "status").orderBy("delivered_date_month_end", "id")
w3 = Window.orderBy("discount", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("order_id").orderBy(F.col("discount").desc_nulls_last())
w5 = Window.partitionBy("order_status")

base = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-08-21").cast("date")) & F.col("status").isin(["LOST", "IN_TRANSIT", "RETURNED"]))
    .withColumn("delivered_date_month_end", F.last_day("delivered_date"))
)

joined = (
    base
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.channel").isin(["web", "partner", "store"]))
    .select("src.*", F.col("b.discount"))
)

joined_shipments = (
    joined
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.status").alias("order_status"))
)

enriched = (
    joined_shipments
    .select(
        "*",
        F.lead("cost", 2, 0).over(w).alias("next_cost"),
        F.row_number().over(w2).alias("row_seq"),
        F.avg("cost").over(w3).alias("cost_avg_6_prior"),
        F.rank().over(w4).alias("discount_rank"),
        F.min("cost").over(w5).alias("order_status_min_cost"),
    )
)

summary = (
    enriched
    .groupBy(
        F.when(F.col("order_status_min_cost") >= 106, "HIGH").otherwise("LOW").alias("order_status_min_cost_tier"),
        "order_status",
        "status",
    )
    .agg(
        F.count_if(F.col("order_status") == "RETURNED").alias("returned_events"),
        F.sum(F.when(F.col("carrier") == "Flash", F.col("discount"))).alias("flash_total_discount"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.size(F.collect_set("status")).alias("num_status_2"),
        F.count("*").alias("event_count"),
        F.avg(F.when(F.col("status") == "LOST", F.col("cost"))).alias("lost_avg_cost"),
    )
    .filter(F.col("event_count") > 1)
)

result = (
    summary
    .select(
        "order_status_min_cost_tier",
        "order_status",
        "status",
        "returned_events",
        "flash_total_discount",
        "num_status",
        "num_status_2",
        "event_count",
        "lost_avg_cost",
    )
    .orderBy(F.col("order_status").desc())
)
