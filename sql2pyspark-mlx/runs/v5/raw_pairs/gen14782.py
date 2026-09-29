from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("segment", "country").orderBy(F.col("signup_date").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter(F.col("segment").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Tokyo"))))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.when(F.col("country").isin(["SG", "DE"]), "Core").otherwise("Other").alias("country_group"),
        F.trim("email").alias("clean_email"),
        F.coalesce("segment", F.lit("DIRECT")).alias("resolved_segment"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "country_group",
    )
)

ranked = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

with_last = (
    ranked
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.country", "s.segment", "s.dedup_rank", F.col("c.account_type"))
)

result = (
    with_last
    .select(
        "id",
        "country",
        "account_type",
        F.when(F.col("account_type").rlike("^(CREDIT|SAVINGS)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("account_type_zone"),
    )
    .orderBy(F.col("account_type").desc_nulls_last())
)
