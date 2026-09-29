from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("order_id")
w_hist = Window.partitionBy("order_id").orderBy("cost", "id")

cleaned = (
    df_shipments
    .filter(F.col("delivered_date") >= "2024-02-01")
    .withColumn("status_lower", F.lower("status"))
)

with_windows = (
    cleaned
    .select(
        "*",
        F.max("cost").over(w).alias("running_max_cost"),
        F.sum("cost").over(w).alias("running_sum_cost"),
        F.count("cost").over(w).alias("running_count_cost"),
    )
)

metrics = (
    with_windows
    .filter(F.col("shipped_date") < "2025-03-01")
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status_lower",
        F.ntile(5).over(w2).alias("cost_bucket"),
        F.max("running_sum_cost").over(w3).alias("order_id_max_running_sum_cost"),
        F.lag("cost", 1).over(w_hist).alias("prev_cost"),
    )
)

cleaned_shipments = (
    metrics
    .filter(F.col("carrier").isNotNull())
    .select(
        "*",
        F.least("cost", "cost_bucket").alias("least_cost_cost_bucket"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_clean"),
    )
)

result = (
    cleaned_shipments
    .withColumn("week_start", F.date_trunc("WEEK", "shipped_date"))
    .withColumn("least_cost_cost_bucket_tier", F.when(F.col("least_cost_cost_bucket") >= 82, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["week_start", "least_cost_cost_bucket_tier"], ["week_start"]],
        "week_start",
        "least_cost_cost_bucket_tier",
    )
    .agg(
        F.count_if(F.col("carrier") == "DHL").alias("dhl_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.countDistinct("carrier").alias("distinct_carriers"),
        F.sum(F.when(F.col("carrier") == "UPS", F.col("order_id_max_running_sum_cost")).otherwise(0)).alias("ups_order_id_max_running_sum_cost"),
        F.grouping_id().alias("grouping_level"),
    )
)
