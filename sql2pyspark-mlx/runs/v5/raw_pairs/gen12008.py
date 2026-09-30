from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date_period", "id")
w2 = Window.partitionBy("carrier").orderBy("delivered_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier").orderBy(F.col("delivered_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_first())
w5 = Window.partitionBy("carrier").orderBy(F.col("session_no").desc())
w6 = Window.partitionBy("activity_month").orderBy(F.col("distinct_carriers").desc())

filtered = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "cost",
        F.date_trunc("month", "delivered_date").alias("delivered_date_period"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.col("delivered_date_period") - F.lag("delivered_date_period").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

remaining_keys = (
    sessionized
    .select("carrier", "id")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date") <= "2025-03-04")
        .select("carrier", "id")
    )
)

reconciled = (
    sessionized
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.max("session_no").over(w3).alias("session_no_max_90d"),
        F.percent_rank().over(w4).alias("cost_percent_rank"),
        F.dense_rank().over(w5).alias("session_no_dense_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("MONTH", "delivered_date_period").alias("activity_month"),
        F.date_trunc("WEEK", "delivered_date_period").alias("week_start"),
    )
    .agg(
        F.countDistinct("carrier").alias("distinct_carriers"),
        F.min("delivered_date_period").alias("first_seen"),
        ((F.unix_timestamp(F.max("delivered_date_period")) - F.unix_timestamp(F.min("delivered_date_period"))) / 60).alias("span_minutes"),
    )
    .withColumn("distinct_carriers_dense_rank", F.dense_rank().over(w6))
    .orderBy(F.col("activity_month").asc_nulls_last())
)
