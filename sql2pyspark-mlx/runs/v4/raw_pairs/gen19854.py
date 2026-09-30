from pyspark.sql import functions as F

remaining_keys = (
    df_customers
    .select("segment", "country")
    .subtract(
        df_customers
        .filter(F.col("city").isNotNull())
        .select("segment", "country")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "name",
        "city",
        "signup_date",
        "age",
        F.when(F.col("name").rlike("^(FRANK MILLER|Kanya Srisuk)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("name_zone"),
    )
    .orderBy(F.col("name_zone").desc_nulls_last())
)
