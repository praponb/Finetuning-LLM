from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy(F.col("cost").desc_nulls_first())
w2 = Window.orderBy("cost", "id").rowsBetween(-2, -1)
w3 = Window.orderBy("shipped_date", "id")
w4 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id", "status").orderBy(F.col("cost_rounded").desc())
w6 = Window.partitionBy("order_id", "carrier").orderBy("cost_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status", "carrier").orderBy("cost_rounded", "id")
w8 = Window.partitionBy("carrier")
w9 = Window.partitionBy("order_id", "carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("cost", "id")
w11 = Window.partitionBy("order_id")

filtered = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("carrier") == "UPS"))
    .withColumn("cost_rounded", F.round("cost", 1))
    .withColumn("carrier_prefix", F.substring("carrier", 1, 3))
)

with_windows = (
    filtered
    .withColumn("cost_cume_dist", F.cume_dist().over(w))
    .withColumn("cost_rounded_avg_2_prior", F.avg("cost_rounded").over(w2))
)

cleaned = (
    with_windows
    .filter(F.col("carrier").isNotNull() & ~F.col("status").isin(["RETURNED", "IN_TRANSIT", "DELIVERED"]))
    .select(
        "*",
        (F.col("cost_rounded_avg_2_prior") + F.col("cost")).alias("cost_rounded_avg_2_prior_plus_cost"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.lag("cost", 1).over(w3).alias("prev_cost"),
        F.sum("cost_rounded_avg_2_prior").over(w4).alias("running_cost_rounded_avg_2_prior"),
        F.cume_dist().over(w5).alias("cost_rounded_cume_dist"),
        F.first("cost_rounded_avg_2_prior", ignorenulls=True).over(w6).alias("first_known_cost_rounded_avg_2_prior"),
    )
)

metrics = (
    enriched
    .filter(F.col("carrier") == "Kerry")
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "cost_rounded",
        "cost_cume_dist",
        "cost_rounded_avg_2_prior",
        "cost_rounded_avg_2_prior_plus_cost",
        "running_cost_rounded_avg_2_prior",
        "first_known_cost_rounded_avg_2_prior",
        F.ntile(5).over(w7).alias("cost_rounded_bucket"),
        F.count("cost_cume_dist").over(w8).alias("carrier_count_cost_cume_dist"),
        F.sum("cost_rounded").over(w9).alias("running_cost_rounded"),
        F.row_number().over(w10).alias("event_seq"),
        F.round((F.col("cost") / F.sum("cost").over(w11)), 4).alias("cost_share"),
    )
)

cleaned_shipments = (
    metrics
    .filter(F.col("event_seq") == 7)
    .select("*", F.floor("cost_rounded").alias("cost_rounded_whole"))
)

result = (
    cleaned_shipments
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "cost_rounded",
        "cost_rounded_avg_2_prior",
        "cost_rounded_avg_2_prior_plus_cost",
        "first_known_cost_rounded_avg_2_prior",
        "cost_rounded_bucket",
    )
    .orderBy(F.col("cost_rounded_avg_2_prior_plus_cost").desc())
)
