from pyspark.sql import functions as F

kept_keys = (
    df_customers
    .select("segment", "country")
    .intersectAll(
        df_customers
        .filter(F.col("country") == "DE")
        .select("segment", "country")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
    )
    .orderBy(F.col("city").asc_nulls_first(), "signup_date")
)
