from pyspark.sql import functions as F

matched_keys = (
    df_stores
    .select("region", "id")
    .subtract(
        df_stores
        .filter(F.col("store_name").like("Mal%"))
        .select("region", "id")
    )
)

kept_rows = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.when(F.col("store_name").rlike("^(Siam Flagship|Mall Central)$"), "GROUP_B").when(F.col("store_name").rlike("^(Downtown Express|Airport Kiosk)$"), "GROUP_A").otherwise("PRIORITY").alias("store_name_zone"),
    )
    .orderBy(F.col("sqft").desc_nulls_first())
)
