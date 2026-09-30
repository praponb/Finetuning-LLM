from pyspark.sql import functions as F

kept_keys = (
    df_customers
    .select("city", "id")
    .exceptAll(
        df_customers
        .filter(F.col("name").like("bob%"))
        .select("city", "id")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.when(F.col("segment").rlike("^(Corporate|SMB)$"), "PRIORITY").when(F.col("segment").rlike("^(Consumer)$"), "EXTENDED").otherwise("CORE").alias("segment_zone"),
    )
)
