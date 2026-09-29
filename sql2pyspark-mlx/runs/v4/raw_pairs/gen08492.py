from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("store_name_length")
w3 = Window.partitionBy("region").orderBy("sqft", "id")
w4 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("region").orderBy("store_name_length", "id")
w_grp = Window.partitionBy("region").orderBy("store_name_length_avg_5", "id")
w7 = Window.partitionBy("region")

valid = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.length("store_name").alias("store_name_length"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_filled"),
    )
)

enriched = (
    valid
    .withColumn("last_known_sqft", F.last("sqft", ignorenulls=True).over(w))
    .withColumn("store_name_length_cume_dist", F.cume_dist().over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("store_name_length_avg_5", F.avg("store_name_length").over(w4))
    .withColumn("prev_store_name_length", F.lag("store_name_length", 2).over(w3))
)

metrics = (
    enriched
    .select(
        "*",
        F.row_number().over(w5).alias("seq_no_2"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.stddev_samp("store_name_length_avg_5").over(w7).alias("region_stddev_samp_store_name_length_avg_5"),
    )
)

base = (
    metrics
    .filter(F.col("region").isNotNull() & F.col("seq_no_2").isNotNull())
    .select(
        "*",
        (F.col("seq_no") / F.nullif("store_name_length_avg_5", F.lit(0))).alias("seq_no_per_store_name_length_avg_5"),
    )
)

totals = (
    base
    .groupBy(
        F.date_trunc("year", "opened_date").alias("year_start"),
        F.when(F.col("seq_no_2") >= 9, "PREMIUM").otherwise("STANDARD").alias("seq_no_2_tier"),
    )
    .agg(
        F.percentile_approx("region_stddev_samp_store_name_length_avg_5", [0.25, 0.5, 0.75]).alias("region_stddev_samp_store_name_length_avg_5_quartiles"),
        F.count("*").alias("cnt"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.bit_and("sqft").alias("sqft_bit_and_2"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "North", 1)), F.lit(0)), 2).alias("sqft_per_north"),
    )
    .filter(F.col("sqft_bit_and_2") >= 1)
)

result = (
    totals
    .select(
        "year_start",
        "seq_no_2_tier",
        "region_stddev_samp_store_name_length_avg_5_quartiles",
        "sqft_bit_and",
        "sqft_bit_and_2",
        "sqft_per_north",
    )
    .orderBy(F.col("sqft_bit_and").desc())
)
