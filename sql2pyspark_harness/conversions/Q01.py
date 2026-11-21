"""Q01 — Web sessionization + JSON payload parsing (reference conversion)."""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

PAYLOAD_SCHEMA = (
    "page STRING, referrer STRING, query STRING, results INT, product_id INT, qty INT, "
    "cart_value DOUBLE, device STRUCT<os: STRING, type: STRING>"
)


def build(spark: SparkSession) -> DataFrame:
    parsed = (
        spark.table("web_events")
        .where(F.col("customer_id").isNotNull())
        .select(
            "event_id", "customer_id", "event_ts", "event_type",
            F.from_json("payload", PAYLOAD_SCHEMA).alias("p"),
            F.get_json_object("payload", "$.device.os").alias("device_os_raw"),
        )
    )

    w = Window.partitionBy("customer_id").orderBy("event_ts", "event_id")
    prev_ts = F.lag("event_ts").over(w)

    gaps = (
        parsed
        .withColumn("prev_ts", prev_ts)
        .withColumn(
            "is_new_session",
            F.when(prev_ts.isNull(), 1)
             .when(F.unix_timestamp("event_ts") - F.unix_timestamp(prev_ts) > 30 * 60, 1)
             .otherwise(0),
        )
    )

    sessionized = gaps.withColumn(
        "session_seq",
        F.sum("is_new_session").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
    )

    # NB: in SQL the literal 60.0 is DECIMAL(3,1), so the division yields a DECIMAL.
    # A Python 60.0 would be DOUBLE and change the result type.
    sixty = F.lit(60.0).cast("decimal(3,1)")

    return (
        sessionized
        .groupBy("customer_id", "session_seq")
        .agg(
            F.min("event_ts").alias("session_start"),
            F.max("event_ts").alias("session_end"),
            F.round((F.unix_timestamp(F.max("event_ts")) - F.unix_timestamp(F.min("event_ts"))) / sixty, 1)
             .alias("duration_min"),
            F.count(F.lit(1)).alias("events"),
            F.countDistinct("event_type").alias("distinct_event_types"),
            F.collect_list("event_type").alias("event_path"),
            F.first(F.col("p.referrer"), ignorenulls=True).alias("entry_referrer"),
            F.coalesce(F.max(F.col("p.device.type")), F.lit("unknown")).alias("device_type"),
            F.max(F.when(F.col("event_type") == "checkout", F.col("p.cart_value"))).alias("checkout_value"),
            (F.max(F.when(F.col("p").isNull() | F.col("p.page").isNull(), 1).otherwise(0)) == 1)
             .alias("has_bad_payload"),
            F.array_join(F.array_distinct(F.collect_list(F.col("p.query"))), " | ").alias("search_terms"),
        )
        .select(
            "customer_id",
            F.concat_ws(
                "-", F.lit("S"),
                F.lpad(F.col("customer_id").cast("string"), 4, "0"),
                F.col("session_seq").cast("string"),
            ).alias("session_key"),
            "session_start", "session_end", "duration_min", "events", "distinct_event_types",
            "event_path", "entry_referrer", "device_type", "checkout_value",
            "has_bad_payload", "search_terms",
        )
        .orderBy("customer_id", "session_start")
    )
