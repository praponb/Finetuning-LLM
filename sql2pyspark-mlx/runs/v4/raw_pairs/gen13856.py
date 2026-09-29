from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("created_at").desc(), "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key")
w3 = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

exploded = df_carts.select("id", "created_at", F.explode("attributes").alias("attr_key", "v"))

latest = (
    exploded
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

gaps = (
    latest
    .withColumn("session_start_flag", F.when(F.col("created_at") - F.lag("created_at").over(w2) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("session_start_flag").over(w3))

with_windows = (
    islands
    .select(
        "*",
        F.sum("session_start_flag").over(w3).alias("cumulative_session_start_flag"),
        F.count("session_start_flag").over(w_part).alias("session_start_flag_count_15m"),
    )
)

result = (
    with_windows
    .withColumn("session_start_flag_tier", F.when(F.col("session_start_flag") >= 1, "PREMIUM").otherwise("STANDARD"))
    .rollup("session_start_flag_tier")
    .agg(
        F.percentile_approx("cumulative_session_start_flag", [0.25, 0.5, 0.75]).alias("cumulative_session_start_flag_quartiles"),
        F.max(F.when(F.col("attr_key") == "weight_kg", F.col("session_start_flag"))).alias("weight_kg_max_session_start_flag"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("weight_kg_max_session_start_flag") < 0)
    .orderBy(F.col("event_count").desc())
)
