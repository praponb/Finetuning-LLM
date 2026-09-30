from pyspark.sql import functions as F

matched_keys = (
    df_suppliers
    .select("city", "country")
    .intersectAll(
        df_suppliers
        .filter(F.col("rating") < 4.0)
        .select("city", "country")
    )
)

verified = (
    df_suppliers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.when(F.col("city").rlike("^(Bangkok|New York)$"), "EXTENDED").when(F.col("city").rlike("^(Chiang Mai|Singapore)$"), "CORE").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("city").desc_nulls_last())
)
