from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc_nulls_first(), "id")
w2 = Window.partitionBy("country").orderBy("is_tablet", "id")
w3 = Window.orderBy(F.col("is_tablet").desc(), "id")
w4 = Window.partitionBy("browser").orderBy("is_tablet", "id")
w5 = Window.partitionBy("country")
w6 = Window.orderBy(F.col("rank_in_group").desc(), "id")
w7 = Window.partitionBy("country").orderBy("started_at", "id")
w8 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter(~F.col("browser").eqNullSafe(F.lit("Chrome")))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.initcap(F.trim("device")).alias("device_title"),
        F.when(F.col("device") == "tablet", 1).otherwise(0).alias("is_tablet"),
        F.coalesce("country", F.lit("NONE")).alias("country_filled"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest
    .withColumn("next_is_tablet", F.lead("is_tablet", 2).over(w2))
    .withColumn("is_tablet_bucket", F.ntile(4).over(w3))
    .withColumn("is_tablet_bucket_2", F.ntile(5).over(w4))
    .withColumn("country_max_is_tablet", F.max("is_tablet").over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
)

gaps = (
    enriched
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w7) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_burst").over(w8))

result = (
    islands
    .groupBy("browser", "device")
    .pivot("country", ["US", "SG"])
    .agg(F.avg("is_tablet"))
)
