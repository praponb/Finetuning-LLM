from pyspark.sql import functions as F

matched_keys = (
    df_customers
    .select("segment", "id")
    .exceptAll(
        df_customers
        .filter(F.col("name").isNotNull())
        .select("segment", "id")
    )
)

verified = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "country", "city", "signup_date")
    .orderBy(F.col("city").desc_nulls_last())
)
