from pyspark.sql import functions as F

remaining_keys = (
    df_customers
    .select("city")
    .subtract(
        df_customers
        .filter(F.col("credit_limit").between(7100, 9700))
        .select("city")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
    )
    .orderBy(F.col("country").desc(), F.col("segment").desc())
)
