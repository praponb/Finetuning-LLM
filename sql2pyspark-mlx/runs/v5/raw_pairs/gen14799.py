from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date_period", "id")
w2 = Window.partitionBy("order_id").orderBy("shipped_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("shipped_date_period", "id")
w4 = Window.orderBy("shipped_date", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("status").orderBy(F.col("island_id_avg_2_prior").desc(), "id")
w8 = Window.partitionBy("status").orderBy("island_id_avg_3_str", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .withColumn("status_lower", F.lower("status"))
    .withColumn("shipped_date_period", F.date_trunc("week", "shipped_date"))
    .withColumn("carrier_clean", F.coalesce("carrier", F.lit("OTHER")))
)

valid = (
    base
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-06-09"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "status_lower",
        "shipped_date_period",
        "carrier_clean",
        F.upper("status").alias("status_upper"),
    )
)

gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.col("shipped_date_period") - F.lag("shipped_date_period").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_shipments = (
    gaps
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "status",
        "status_lower",
        "shipped_date_period",
        "carrier_clean",
        "status_upper",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

metrics = (
    sessions_shipments
    .filter(F.year("shipped_date") >= 2023)
    .select(
        "id",
        "status",
        "status_lower",
        "status_upper",
        "island_id",
        F.row_number().over(w3).alias("seq_no"),
        F.avg("island_id").over(w4).alias("island_id_avg_3"),
        F.last("carrier", ignorenulls=True).over(w5).alias("last_known_carrier"),
        F.avg("island_id").over(w6).alias("island_id_avg_2_prior"),
    )
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

valid_shipments = (
    ranked
    .filter(F.col("island_id_avg_2_prior") < 3)
    .select("*", F.col("island_id_avg_3").cast("double").alias("island_id_avg_3_str"))
)

result = (
    valid_shipments
    .select(
        "id",
        "status",
        "status_lower",
        "island_id",
        "seq_no",
        "last_known_carrier",
        "dedup_rank",
        F.avg("island_id_avg_2_prior").over(w8).alias("running_avg_island_id_avg_2_prior"),
        F.sum("island_id_avg_2_prior").over(w8).alias("running_sum_island_id_avg_2_prior"),
        F.min("island_id_avg_2_prior").over(w8).alias("running_min_island_id_avg_2_prior"),
    )
)
