from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city", "region").orderBy("least_sqft_dedup_rank", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy("dedup_rank", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-10-01"))) & (F.col("sqft") == 12632))
    .withColumn("store_name_lower", F.lower("store_name"))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") <= F.col("s.opened_date")), "left_semi")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.opened_date")), "left_semi")
)

ranked = (
    eligible
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered_stores = (
    ranked
    .filter((F.col("region").eqNullSafe(F.lit("North"))) & F.col("region").isNotNull())
    .withColumn("least_sqft_dedup_rank", F.least("sqft", "dedup_rank"))
    .withColumn("resolved_region", F.coalesce("region", F.lit("OTHER")))
)

with_windows = (
    filtered_stores
    .select(
        "*",
        F.avg("sqft").over(w2).alias("sqft_avg_5"),
        F.avg("sqft").over(w3).alias("sqft_avg_3"),
    )
)

remaining_keys = (
    with_windows
    .select("region", "id")
    .subtract(
        df_stores
        .filter(F.col("city") == "Tokyo")
        .select("region", "id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "store_name",
        "city",
        "opened_date",
        "sqft",
        "store_name_lower",
        "dedup_rank",
        "least_sqft_dedup_rank",
        "resolved_region",
        "sqft_avg_5",
        F.when(F.col("city").rlike("^(New York|Tokyo)$"), "CORE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "PRIORITY").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").asc_nulls_first())
)
