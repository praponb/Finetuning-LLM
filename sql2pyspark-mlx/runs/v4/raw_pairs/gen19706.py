from pyspark.sql import functions as F

kept_keys = (
    df_customers
    .select("id")
    .subtract(
        df_customers
        .filter(~F.col("city").isin(["Berlin", "New York"]))
        .select("id")
    )
)

verified = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "name", "country", "segment", "signup_date", "credit_limit", "age")
    .orderBy(F.col("credit_limit").desc_nulls_last())
)
