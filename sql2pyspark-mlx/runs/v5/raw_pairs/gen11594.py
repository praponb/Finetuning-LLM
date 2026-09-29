from pyspark.sql import functions as F

matched_keys = (
    df_stores
    .select("region", "id")
    .exceptAll(
        df_stores
        .filter(F.col("city").isin(["Berlin", "Singapore"]))
        .select("region", "id")
    )
)

reconciled = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.when(F.col("region").rlike("^(Central|East)$"), "GROUP_A").when(F.col("region").rlike("^(North|South)$"), "CORE").otherwise("OTHER_ZONE").alias("region_zone"),
    )
    .orderBy(F.col("id").desc())
)
