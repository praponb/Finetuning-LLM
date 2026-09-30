from pyspark.sql import functions as F

remaining_keys = (
    df_stores
    .select("id", "city")
    .subtract(
        df_stores
        .filter(F.col("sqft").between(5232, 11992))
        .select("id", "city")
    )
)

reconciled = (
    df_stores
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.when(F.col("city").rlike("^(Berlin|New York)$"), "GROUP_B").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_A").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("city").asc_nulls_first())
)
