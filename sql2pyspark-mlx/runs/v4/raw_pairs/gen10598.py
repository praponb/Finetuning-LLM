from pyspark.sql import functions as F

kept_keys = (
    df_customers
    .select("country", "city")
    .intersectAll(
        df_customers
        .filter(F.col("city") != "Berlin")
        .select("country", "city")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
    )
)
