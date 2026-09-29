from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date")
w2 = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc())
w3 = Window.partitionBy("carrier").orderBy(F.col("adjusted_cost").desc_nulls_first())
w4 = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("carrier")
w6 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy("cost", "id")
w8 = Window.partitionBy("status").orderBy("last7_max_last_known_adjusted_cost", "id")
w9 = Window.partitionBy("order_id").orderBy("adjusted_cost_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w_recent = Window.orderBy(F.col("shipped_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_ordered = Window.partitionBy("order_id").orderBy("carrier_sum_cost", "id")

valid = (
    df_shipments
    .filter(~F.col("carrier").eqNullSafe(F.lit("Kerry")))
    .select(
        "*",
        F.date_trunc("year", "shipped_date").alias("shipped_date_period"),
        F.round((F.col("cost") * 1.15), 2).alias("adjusted_cost"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

deduped = (
    ranked
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

metrics = (
    deduped
    .withColumn("adjusted_cost_percent_rank", F.percent_rank().over(w3))
    .withColumn("last_known_adjusted_cost", F.last("adjusted_cost", ignorenulls=True).over(w4))
    .withColumn("carrier_sum_cost", F.sum("cost").over(w5))
)

enriched = (
    metrics
    .filter(F.col("cost") < 58)
    .withColumn("last7_min_last_known_adjusted_cost", F.min("last_known_adjusted_cost").over(w6))
    .withColumn("last7_count_last_known_adjusted_cost", F.count("last_known_adjusted_cost").over(w6))
    .withColumn("last7_max_last_known_adjusted_cost", F.max("last_known_adjusted_cost").over(w6))
    .withColumn("prev_cost", F.lag("cost", 1).over(w7))
)

metrics_shipments = (
    enriched
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("first_known_adjusted_cost", F.first("adjusted_cost", ignorenulls=True).over(w9))
    .withColumn("next_cost", F.lead("cost", 2, 0).over(w_running))
    .withColumn("carrier_sum_cost_max_90d", F.max("carrier_sum_cost").over(w_recent))
    .withColumn("prev_prev_cost", F.lag("prev_cost", 2).over(w_ordered))
)

wide = (
    metrics_shipments
    .groupBy("status")
    .pivot("carrier", ["DHL", "Flash", "UPS"])
    .agg(
        F.sum("last7_count_last_known_adjusted_cost").alias("value_sum"),
        F.count(F.lit(1)).alias("n"),
    )
    .select(
        "status",
        F.col("DHL_value_sum").alias("dhl_value_sum"),
        F.col("DHL_n").alias("dhl_n"),
        F.col("Flash_value_sum").alias("flash_value_sum"),
        F.col("Flash_n").alias("flash_n"),
        F.col("UPS_value_sum").alias("ups_value_sum"),
        F.col("UPS_n").alias("ups_n"),
    )
)

result = (
    wide
    .select(
        "status",
        "dhl_value_sum",
        "dhl_n",
        "flash_value_sum",
        "flash_n",
        "ups_value_sum",
        "ups_n",
    )
    .orderBy(F.col("dhl_n").asc_nulls_first())
)
