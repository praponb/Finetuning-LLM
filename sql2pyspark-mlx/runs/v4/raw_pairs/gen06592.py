from pyspark.sql import functions as F

kept_keys = (
    df_customers
    .select("country", "segment")
    .exceptAll(
        df_customers
        .filter(F.col("segment").isin(["SMB", "Consumer"]))
        .select("country", "segment")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("name", "email", "country", "segment", "is_active", "age")
    .orderBy(F.col("segment").asc_nulls_last(), "country")
)
