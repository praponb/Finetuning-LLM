from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date_plus_months_minus_17d", "id")
w2 = Window.partitionBy("region").orderBy("opened_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region").orderBy("opened_date_plus_months_minus_17d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        F.add_months("opened_date", 12).alias("opened_date_plus_months"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

valid = (
    cleaned
    .filter(F.length("store_name") >= 6)
    .select(
        "id",
        "store_name",
        "region",
        "opened_date_plus_months",
        "region_clean",
        F.concat_ws("/", "region", "store_name").alias("region_store_name_key"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.shipping_fee") < 36), "left_anti")
)

valid_stores = (
    kept
    .filter(~F.col("region").eqNullSafe(F.lit("West")))
    .withColumn("opened_date_plus_months_minus_17d", F.date_sub("opened_date_plus_months", 17))
    .withColumn("region_filled", F.coalesce("region", F.lit("NONE")))
)

scored = (
    valid_stores
    .filter(F.month("opened_date_plus_months_minus_17d").isin([3, 7, 11]))
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.first("region", ignorenulls=True).over(w2).alias("first_known_region"),
        F.row_number().over(w).alias("event_seq"),
        F.first("region", ignorenulls=True).over(w3).alias("first_known_region_2"),
    )
)

remaining_keys = (
    scored
    .select("id", "region")
    .exceptAll(
        df_stores
        .filter(~F.col("region").isin(["North", "South", "East"]))
        .select("id", "region")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date_plus_months"))
    .groupingSets(
        [["region", "activity_month"], ["region"], []],
        "region",
        "activity_month",
    )
    .agg(
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("avg_event_seq") < 1000000)
    .orderBy(F.col("avg_row_seq").desc())
)
