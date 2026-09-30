from pyspark.sql import functions as F

matched_keys = (
    df_suppliers
    .select("id", "city")
    .intersectAll(
        df_suppliers
        .filter(F.col("city") != "Tokyo")
        .select("id", "city")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.when(F.col("country").rlike("^(UK|SG)$"), "GROUP_B").when(F.col("country").rlike("^(TH|US)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("country_zone"),
    )
)
