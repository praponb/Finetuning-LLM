from pyspark.sql import functions as F

remaining_keys = (
    df_customers
    .select("id")
    .intersect(
        df_customers
        .filter(F.col("is_active"))
        .select("id")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("segment", "signup_date", "is_active", "credit_limit", "age")
    .orderBy(F.col("signup_date").asc_nulls_first())
)
