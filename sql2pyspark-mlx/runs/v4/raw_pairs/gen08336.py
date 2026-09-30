from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter(F.col("region").isNotNull() & F.col("city").isin(["Chiang Mai", "Tokyo", "Berlin"]))
    .withColumn("opened_date_period", F.date_trunc("month", "opened_date"))
)

matched_keys = (
    filtered
    .select("id", "region")
    .subtract(
        df_stores
        .filter(F.col("region").isin(["North", "South"]))
        .select("id", "region")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    kept_rows
    .select("region", "id")
    .intersect(
        df_stores
        .filter(F.year("opened_date") == 2024)
        .select("region", "id")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") <= F.col("s.opened_date_period")), "left_semi")
)

result = not_flagged.select("id", "city", "region")
