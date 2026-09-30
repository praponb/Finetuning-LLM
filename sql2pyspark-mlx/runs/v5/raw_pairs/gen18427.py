from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc_nulls_first(), "id")

filtered = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-08-17").cast("date"))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.year("last_restock").alias("last_restock_year"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

enriched = (
    filtered
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .filter(F.col("p.store_name").isNull())
    .select(
        "e.id",
        "e.store_id",
        "e.last_restock",
        F.col("p.region"),
        F.col("p.opened_date"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

filtered_inventory = (
    ranked
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "last_restock",
        "region",
        "opened_date",
        "rn",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

matched_keys = (
    filtered_inventory
    .select("id")
    .exceptAll(
        df_inventory
        .filter(F.col("last_restock") <= "2023-05-27")
        .select("id")
    )
)

reconciled = (
    filtered_inventory
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.date_trunc("YEAR", "last_restock").alias("year_start"),
        F.date_trunc("MONTH", "last_restock").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_rows"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("region") == "Central").alias("central_events"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.count_if(F.col("region") == "East").alias("east_events"),
    )
    .orderBy(F.col("activity_month").desc_nulls_first())
)
