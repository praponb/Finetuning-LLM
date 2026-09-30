from pyspark.sql import functions as F

kept_keys = (
    df_customers
    .select("segment", "city")
    .subtract(
        df_customers
        .filter(F.length("name") > 6)
        .select("segment", "city")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "name",
        "email",
        "signup_date",
        "is_active",
        "credit_limit",
        F.when(F.col("name").rlike("^(Mia Rossi|bob smith)$"), "GROUP_B").otherwise("EXTENDED").alias("name_zone"),
    )
    .orderBy(F.col("credit_limit").asc_nulls_first())
)
