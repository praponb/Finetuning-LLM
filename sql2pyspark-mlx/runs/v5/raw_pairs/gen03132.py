from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost_whole").desc_nulls_last())
w2 = Window.orderBy(F.col("cost_whole").desc_nulls_first())
w3 = Window.partitionBy("carrier")
w4 = Window.orderBy(F.col("cost_whole_rank").desc_nulls_last())
w5 = Window.partitionBy("carrier").orderBy(F.col("cost_whole_rank_percent_rank").desc_nulls_first())
w6 = Window.partitionBy("carrier").orderBy(F.col("cost_whole_percent_rank").asc_nulls_first())
w7 = Window.partitionBy("carrier").orderBy("carrier_avg_cost_whole")

filtered = (
    df_shipments
    .filter(F.col("shipped_date") > F.lit("2023-03-17").cast("date"))
    .withColumn("cost_whole", F.ceil("cost"))
    .withColumn("status_title", F.initcap(F.trim("status")))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("N/A")))
)

matched_keys = (
    filtered
    .select("status")
    .exceptAll(
        df_shipments
        .filter(F.col("delivered_date") > "2025-08-01")
        .select("status")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id").eqNullSafe(F.col("b.id")), "inner")
    .select("a.*", F.col("b.channel"))
)

cleaned = (
    joined
    .filter((F.col("delivered_date") >= F.lit("2023-06-28").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "cost_whole",
        "status_title",
        "resolved_carrier",
        F.date_add("delivered_date", 25).alias("delivered_date_plus_25d"),
    )
)

metrics = (
    cleaned
    .filter(F.col("cost") < 90)
    .select(
        "carrier",
        "delivered_date",
        "cost_whole",
        F.percent_rank().over(w).alias("cost_whole_percent_rank"),
        F.rank().over(w2).alias("cost_whole_rank"),
        F.stddev_samp("cost_whole").over(w3).alias("carrier_stddev_samp_cost_whole"),
    )
)

enriched = (
    metrics
    .select(
        "carrier",
        "cost_whole_percent_rank",
        F.percent_rank().over(w4).alias("cost_whole_rank_percent_rank"),
        F.avg("cost_whole").over(w3).alias("carrier_avg_cost_whole"),
    )
)

enriched_shipments = (
    enriched
    .withColumn("cost_whole_rank_percent_rank_percent_rank", F.percent_rank().over(w5))
    .withColumn("cost_whole_percent_rank_rank", F.rank().over(w6))
    .withColumn("carrier_avg_cost_whole_dense_rank", F.dense_rank().over(w7))
)

totals = (
    enriched_shipments
    .cube("carrier")
    .agg(
        F.sum(F.when(F.col("carrier") == "UPS", F.col("cost_whole_rank_percent_rank_percent_rank"))).alias("ups_total_cost_whole_rank_percent_rank_percent_rank"),
        F.percentile_approx("carrier_avg_cost_whole", [0.25, 0.5, 0.75]).alias("carrier_avg_cost_whole_quartiles"),
        F.grouping("carrier").alias("grouping_id"),
    )
    .filter(F.col("ups_total_cost_whole_rank_percent_rank_percent_rank") > 0)
)

result = totals.select("carrier_avg_cost_whole_quartiles", "grouping_id")
