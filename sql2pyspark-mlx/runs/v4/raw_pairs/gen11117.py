from pyspark.sql import functions as F

kept_keys = (
    df_stores
    .select("region")
    .intersectAll(
        df_stores
        .filter(F.col("region") == "Central")
        .select("region")
    )
)

reconciled = (
    df_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "store_name",
        "region",
        "sqft",
        F.when(F.col("region").rlike("^(West|North)$"), "PRIORITY").when(F.col("region").rlike("^(South|East)$"), "EXTENDED").otherwise("GROUP_A").alias("region_zone"),
    )
)
