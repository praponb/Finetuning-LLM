from pyspark.sql import functions as F

kept_keys = (
    df_stores
    .select("city")
    .subtract(
        df_stores
        .filter(~F.col("store_name").like("D%"))
        .select("city")
    )
)

verified = (
    df_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "store_name",
        "region",
        F.when(F.col("store_name").rlike("^(Siam Flagship|Riverside)$"), "GROUP_B").when(F.col("store_name").rlike("^(Downtown Express|Airport Kiosk)$"), "OTHER_ZONE").otherwise("CORE").alias("store_name_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
