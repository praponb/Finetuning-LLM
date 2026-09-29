from pyspark.sql import functions as F

matched_keys = (
    df_stores
    .select("id", "city")
    .exceptAll(
        df_stores
        .filter(F.col("opened_date").between("2023-04-02", "2024-02-01"))
        .select("id", "city")
    )
)

verified = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.when(F.col("region").rlike("^(Central|South)$"), "PRIORITY").when(F.col("region").rlike("^(North|East)$"), "GROUP_B").otherwise("CORE").alias("region_zone"),
    )
    .orderBy(F.col("region").desc_nulls_first(), F.col("sqft").desc())
)
