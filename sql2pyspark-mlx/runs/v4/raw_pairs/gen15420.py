from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("cost", "id")
w4 = Window.partitionBy("order_id").orderBy("delivered_date_period", "id")
w5 = Window.partitionBy("order_id").orderBy("delivered_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_first(), "id")
w7 = Window.partitionBy("order_id").orderBy("burst_id", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.upper("carrier").alias("carrier_upper"),
        F.date_trunc("month", "delivered_date").alias("delivered_date_period"),
    )
)

with_dims = (
    filtered
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .select("e.*", F.col("o.amount"))
)

enriched = (
    with_dims
    .select(
        "*",
        F.last("cost", ignorenulls=True).over(w).alias("last_known_cost"),
        F.sum("amount").over(w_ordered).alias("cumulative_amount"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

with_gaps = (
    enriched
    .withColumn("new_group_flag", F.when(F.lag("delivered_date_period").over(w4).isNull() | (F.col("delivered_date_period") > F.lag("delivered_date_period").over(w4) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w5))

ranked = (
    sessionized
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

scored = (
    ranked
    .withColumn("amount_avg_3", F.avg("amount").over(w7))
    .withColumn("running_amount", F.sum("amount").over(w8))
)

result = (
    scored
    .groupBy("carrier", F.date_trunc("WEEK", "shipped_date").alias("week_start"))
    .agg(
        F.min("delivered_date_period").alias("first_seen"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.count_if(F.col("status") == "IN_TRANSIT").alias("in_transit_events"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.round(F.avg("last_known_cost"), 2).alias("avg_last_known_cost"),
    )
)
