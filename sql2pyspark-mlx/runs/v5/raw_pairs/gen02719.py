from pyspark.sql import functions as F

matched_keys = (
    df_customers
    .select("id", "city")
    .subtract(
        df_customers
        .filter(F.col("signup_date").between("2024-05-29", "2025-10-01"))
        .select("id", "city")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
    )
    .orderBy("tokyo_rows")
)
