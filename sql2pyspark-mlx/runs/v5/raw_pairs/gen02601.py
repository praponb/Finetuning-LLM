from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w_ordered = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("region", "city")
w4 = Window.partitionBy("city", "region").orderBy("opened_date_minus_79d", "id")

base = (
    df_stores
    .filter((F.col("region").eqNullSafe(F.lit("Central"))) & F.col("region").isNotNull())
    .withColumn("opened_date_minus_79d", F.date_sub("opened_date", 79))
    .withColumn("opened_date_plus_months", F.add_months("opened_date", 3))
    .withColumn("region_clean", F.coalesce("region", F.lit("N/A")))
)

not_flagged = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.units") == 13), "left_semi")
)

with_windows = (
    not_flagged
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date_minus_79d",
        "region_clean",
        F.lag("sqft", 1).over(w).alias("prev_sqft"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
        F.row_number().over(w4).alias("seq_no_2"),
    )
)

valid = (
    with_windows
    .filter(F.col("region").isNotNull() & (~F.col("region").eqNullSafe(F.lit("South"))))
    .select(
        "id",
        "city",
        "region",
        "region_clean",
        "prev_sqft",
        "seq_no",
        "sqft_share",
        "seq_no_2",
        (F.col("sqft_share") * F.col("prev_sqft")).alias("sqft_share_times_prev_sqft"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

result = (
    valid
    .withColumn("prev_sqft_tier", F.when(F.col("prev_sqft") >= 13338, "PREMIUM").otherwise("STANDARD"))
    .cube("prev_sqft_tier", "region")
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg("seq_no_2"), 2).alias("avg_seq_no_2"),
        F.avg(F.when(F.col("region") == "North", F.col("seq_no_2"))).alias("north_avg_seq_no_2"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("num_rows") > 1)
)
