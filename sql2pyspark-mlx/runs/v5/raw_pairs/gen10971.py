from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country", "browser").orderBy(F.col("started_at").desc_nulls_first(), "id")
w3 = Window.partitionBy("country").orderBy("started_at", "id")
w4 = Window.partitionBy("country", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("country").orderBy("row_seq", "id")
w8 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.orderBy("row_seq", "id")
w10 = Window.partitionBy("country")
w11 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

with_windows = (
    df_sessions
    .filter(F.col("country") == "TH")
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w))
)

valid = (
    with_windows
    .filter((F.col("country").eqNullSafe(F.lit("US"))) & (F.col("started_at") > F.lit("2023-04-05").cast("date")))
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.concat("device", F.lit("/"), "browser").alias("device_browser"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)

ranked = (
    valid
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    ranked
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        "device_browser",
        F.row_number().over(w3).alias("row_seq"),
        F.first("country", ignorenulls=True).over(w4).alias("first_known_country"),
        F.last("country", ignorenulls=True).over(w5).alias("last_known_country"),
        F.last("country", ignorenulls=True).over(w6).alias("last_known_country_2"),
    )
)

enriched = (
    metrics
    .withColumn("next_row_seq", F.lead("row_seq", 2).over(w7))
    .withColumn("row_seq_avg_3", F.avg("row_seq").over(w8))
    .withColumn("next_row_seq_2", F.lead("row_seq", 2).over(w9))
    .withColumn("row_seq_share", F.round((F.col("row_seq") / F.sum("row_seq").over(w10)), 4))
    .withColumn("row_seq_sum_90d", F.sum("row_seq").over(w11))
)

result = (
    enriched
    .groupBy(
        "country",
        F.when(F.col("next_row_seq") >= 9, "PREMIUM").otherwise("STANDARD").alias("next_row_seq_tier"),
    )
    .agg(
        F.min("started_at").alias("first_seen"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.max("row_seq_share").alias("max_row_seq_share"),
    )
)
