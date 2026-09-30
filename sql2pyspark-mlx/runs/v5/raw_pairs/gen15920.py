from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w3 = Window.partitionBy("status").orderBy(F.col("running_count_cost").asc_nulls_last(), "id")
w4 = Window.partitionBy("order_id").orderBy("greatest_running_min_cost_discount", "id")
w5 = Window.partitionBy("order_id").orderBy("rank_in_group", "id").rowsBetween(-3, -1)

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select("*", F.upper("carrier").alias("carrier_upper"))
)

enriched = (
    cleaned
    .select(
        "*",
        F.count("cost").over(w).alias("running_count_cost"),
        F.min("cost").over(w).alias("running_min_cost"),
        F.sum("cost").over(w).alias("running_sum_cost"),
        F.max("cost").over(w).alias("running_max_cost"),
        F.lag("cost", 1, 0).over(w2).alias("prev_cost"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    deduped
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.shipped_date",
        "s.delivered_date",
        "s.carrier_upper",
        "s.running_count_cost",
        "s.running_min_cost",
        "s.prev_cost",
        "s.rank_in_group",
        F.col("b.discount"),
    )
)

base = (
    joined
    .filter((F.col("shipped_date") > F.lit("2023-02-21").cast("date")) & F.col("discount").between(0.32, 0.37))
    .select(
        "id",
        "order_id",
        "carrier_upper",
        "running_count_cost",
        "rank_in_group",
        F.greatest("running_min_cost", "discount").alias("greatest_running_min_cost_discount"),
    )
)

enriched_shipments = (
    base
    .filter(F.col("order_id") == 23)
    .select(
        "id",
        "greatest_running_min_cost_discount",
        F.lag("running_count_cost").over(w4).alias("prev_running_count_cost"),
        F.lag("greatest_running_min_cost_discount").over(w4).alias("prev_greatest_running_min_cost_discount"),
        F.row_number().over(w4).alias("event_seq"),
        F.avg("running_count_cost").over(w5).alias("running_count_cost_avg_3_prior"),
    )
)

result = (
    enriched_shipments
    .groupBy(
        F.when(F.col("greatest_running_min_cost_discount") >= 78, "LARGE").otherwise("SMALL").alias("greatest_running_min_cost_discount_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
        F.min("greatest_running_min_cost_discount").alias("min_greatest_running_min_cost_discount"),
    )
)
