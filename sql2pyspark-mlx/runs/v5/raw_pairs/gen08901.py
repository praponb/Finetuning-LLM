from pyspark.sql import functions as F

remaining_keys = (
    df_suppliers
    .select("id", "country")
    .exceptAll(
        df_suppliers
        .filter(F.col("rating").isNull())
        .select("id", "country")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "city",
        "rating",
        F.when(F.col("city").rlike("^(London|Berlin)$"), "GROUP_A").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").desc_nulls_last())
)
