from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date_period", "id")
w2 = Window.partitionBy("carrier").orderBy("shipped_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier").orderBy(F.col("shipped_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("cost", "id")
w6 = Window.partitionBy("status")
w7 = Window.partitionBy("status", "carrier").orderBy(F.col("shipped_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w8 = Window.orderBy(F.col("prev_is_new_burst").asc_nulls_last())
w9 = Window.partitionBy("carrier").orderBy("cost")
w10 = Window.orderBy(F.col("shipped_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w11 = Window.partitionBy("carrier").orderBy(F.col("session_id_sum_30d").desc_nulls_last())
w_recent = Window.partitionBy("status", "carrier")

base = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-09-21").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.date_format("delivered_date", "yyyy-MM-dd").alias("delivered_date_label"),
        F.date_trunc("YEAR", "shipped_date").alias("shipped_date_period"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_clean"),
    )
)

gaps = (
    base
    .withColumn("is_new_burst", F.when(F.col("shipped_date_period") - F.lag("shipped_date_period").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

enriched = (
    islands
    .withColumn("first_known_carrier", F.first("carrier", ignorenulls=True).over(w3))
    .withColumn("cost_avg_90d", F.avg("cost").over(w4))
    .withColumn("prev_is_new_burst", F.lag("is_new_burst", 2).over(w5))
    .withColumn("status_min_session_id", F.min("session_id").over(w6))
    .withColumn("session_id_sum_30d", F.sum("session_id").over(w7))
)

kept_keys = (
    enriched
    .select("status", "id")
    .intersect(
        df_shipments
        .filter(F.month("delivered_date").isin([3, 6, 8]))
        .select("status", "id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_shipments = (
    reconciled
    .select(
        "*",
        F.cume_dist().over(w8).alias("prev_is_new_burst_cume_dist"),
        F.dense_rank().over(w9).alias("cost_dense_rank"),
        F.sum("cost").over(w10).alias("cost_sum_30d"),
    )
)

scored = (
    enriched_shipments
    .select(
        "*",
        F.rank().over(w11).alias("session_id_sum_30d_rank"),
        F.min("session_id_sum_30d").over(w_recent).alias("status_min_session_id_sum_30d"),
        F.max("cost_dense_rank").over(w10).alias("cost_dense_rank_max_30d"),
    )
)

result = (
    scored
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        "delivered_date_label",
        "shipped_date_period",
        "carrier_clean",
        "is_new_burst",
        F.when(F.col("carrier").rlike("^(UPS|Flash)$"), "GROUP_B").otherwise("PRIORITY").alias("carrier_zone"),
    )
    .orderBy(F.col("is_new_burst").asc_nulls_last())
)
