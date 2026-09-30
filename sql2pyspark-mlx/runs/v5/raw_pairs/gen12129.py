from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("status_length").desc_nulls_first())
w2 = Window.partitionBy("status").orderBy("delivered_date_period", "id")
w3 = Window.partitionBy("status").orderBy("delivered_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("rank_in_group", "id")

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("shipped_date") < "2023-06-01"))
    .select(
        "id",
        "delivered_date",
        "status",
        F.concat_ws(",", "status", "carrier").alias("status_carrier_key"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

filtered = (
    cleaned
    .filter(F.year("delivered_date") == 2024)
    .select(
        "id",
        "status",
        "status_carrier_key",
        F.date_trunc("WEEK", "delivered_date").alias("delivered_date_period"),
        F.length("status").alias("status_length"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

flagged = (
    latest
    .withColumn("is_new_burst", F.when(F.lag("delivered_date_period").over(w2).isNull() | (F.col("delivered_date_period") > F.lag("delivered_date_period").over(w2) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "status",
        "status_carrier_key",
        "delivered_date_period",
        "rank_in_group",
        F.sum("is_new_burst").over(w3).alias("burst_id"),
    )
)

flagged_2 = (
    islands
    .withColumn("session_start_flag", F.when(F.lag("delivered_date_period").over(w2).isNull() | (F.col("delivered_date_period") > F.lag("delivered_date_period").over(w2) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged_2.withColumn("session_id", F.sum("session_start_flag").over(w3))

deduped = (
    sessionized
    .withColumn("rank_in_group_2", F.row_number().over(w4))
    .filter(F.col("rank_in_group_2") == 1)
    .select("id", "status", "rank_in_group", "burst_id")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("burst_id") >= 6, "PREMIUM").otherwise("STANDARD").alias("burst_id_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.min("burst_id").alias("min_burst_id"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"),
        F.countDistinct("status").alias("distinct_status"),
        F.round(F.sum("burst_id") / F.nullif(F.count(F.when(F.col("status") == "DELIVERED", 1)), F.lit(0)), 2).alias("burst_id_per_delivered"),
    )
    .filter(F.col("returned_count") > 1)
)
