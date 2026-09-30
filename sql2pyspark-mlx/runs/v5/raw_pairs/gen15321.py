from pyspark.sql import functions as F

kept_keys = (
    df_stores
    .select("id", "region")
    .exceptAll(
        df_stores
        .filter(F.col("sqft") < 11798)
        .select("id", "region")
    )
)

kept_rows = (
    df_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.when(F.col("region").rlike("^(North|South)$"), "GROUP_B").when(F.col("region").rlike("^(East|West)$"), "CORE").otherwise("GROUP_A").alias("region_zone"),
    )
    .orderBy(F.col("opened_date").desc())
)
