from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("created_at", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("key").orderBy(F.col("created_at").desc(), "id", "key")
w3 = Window.partitionBy("key").orderBy(F.col("event_seq").asc_nulls_first())
w4 = Window.partitionBy("key").orderBy(F.col("row_seq").asc_nulls_last(), "id", "key")

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

with_windows = (
    flattened
    .withColumn("last_known_key", F.last("key", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("first_known_key", F.first("key", ignorenulls=True).over(w))
    .withColumn("first_known_key_2", F.first("key", ignorenulls=True).over(w))
)

deduped = (
    with_windows
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped_carts = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "coupon_codes", "key", "row_seq")
)

latest = (
    deduped_carts
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

stats = (
    latest
    .groupBy("key")
    .agg(
        F.count_if(F.col("key") == "device").alias("device_events"),
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("key") == "utm_source", 1)), F.lit(0)), 2).alias("row_seq_per_utm_source"),
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("key") == "gift", 1)), F.lit(0)), 2).alias("row_seq_per_gift"),
        F.countDistinct("key").alias("distinct_keys"),
    )
)

result = (
    stats
    .select(
        "key",
        "device_events",
        "row_seq_per_gift",
        "distinct_keys",
        F.when(F.col("key").rlike("^(device|gift)$"), "CORE").when(F.col("key").rlike("^(utm_source|weight_kg)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("key_zone"),
    )
    .orderBy(F.col("device_events").desc())
)
