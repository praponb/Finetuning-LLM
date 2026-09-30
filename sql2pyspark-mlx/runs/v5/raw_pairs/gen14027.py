from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id").orderBy("units_scaled", "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("units").desc_nulls_first())
w4 = Window.partitionBy("store_id").orderBy(F.col("units").asc_nulls_first(), "id")
w5 = Window.partitionBy("store_id")
w6 = Window.partitionBy("store_id").orderBy("next_units", "id")
w_running = Window.partitionBy("store_id").orderBy(F.col("units").asc_nulls_last())

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-07-08").cast("date"))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        F.least("revenue", "units").alias("least_revenue_units"),
        F.round((F.col("units") * 0.85), 2).alias("units_scaled"),
    )
)

scored = (
    valid
    .withColumn("next_units", F.lead("units", 1).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("units_rank", F.rank().over(w3))
)

ranked = (
    scored
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

metrics = (
    ranked
    .select(
        "id",
        "store_id",
        "units",
        "least_revenue_units",
        "units_scaled",
        "seq_no",
        "rn",
        F.min("units_scaled").over(w5).alias("store_id_min_units_scaled"),
        F.ntile(5).over(w6).alias("next_units_bucket"),
        F.rank().over(w_running).alias("units_rank_2"),
    )
)

remaining_keys = (
    metrics
    .select("id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("units") > 29)
        .select("id", "store_id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("store_id_min_units_scaled_tier", F.when(F.col("store_id_min_units_scaled") >= 82, "HIGH").otherwise("LOW"))
    .withColumn("seq_no_tier", F.when(F.col("seq_no") >= 7, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["store_id_min_units_scaled_tier", "seq_no_tier"], ["store_id_min_units_scaled_tier"]],
        "store_id_min_units_scaled_tier",
        "seq_no_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("units").alias("units_stddev"),
        F.percentile_approx("units_scaled", [0.25, 0.5, 0.75]).alias("units_scaled_quartiles"),
    )
)
