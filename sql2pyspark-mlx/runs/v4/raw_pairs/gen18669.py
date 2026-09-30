from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("island_id").orderBy(F.col("session_start_flag_per_cart").desc())

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.length("event_type").alias("event_type_length"),
    )
)

gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy(
        "island_id",
        F.when(F.col("event_type_length") >= 65, "LARGE").otherwise("SMALL").alias("event_type_length_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.max(F.when(F.col("event_type") == "add_to_cart", F.col("island_id"))).alias("add_to_cart_max_island_id"),
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("page") == "/cart", 1)), F.lit(0)), 2).alias("session_start_flag_per_cart"),
    )
    .withColumn("session_start_flag_per_cart_rank", F.rank().over(w3))
    .orderBy(F.col("event_type_length_tier").desc())
)
