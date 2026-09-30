from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

base = (
    df_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-05-04")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.concat("name", F.lit("/"), "city").alias("name_city"),
    )
)

base_customers = (
    base
    .filter((F.col("age") <= 38) & (F.col("signup_date") > F.to_date(F.lit("2023-07-19"))))
    .select(
        "id",
        "name",
        "email",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        F.lower("email").alias("email_lower"),
    )
)

latest_match = (
    base_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.opened_date"))
)

kept_keys = (
    latest_match
    .select("id", "segment")
    .intersectAll(
        df_customers
        .filter(F.col("name").like("Oli%"))
        .select("id", "segment")
    )
)

kept_rows = (
    latest_match
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("is_active"))
    .select(
        "email",
        "segment",
        "is_active",
        "credit_limit",
        "email_lower",
        "opened_date",
        F.upper("segment").alias("segment_upper"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("credit_limit") >= 17500, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
    )
    .agg(
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"),
        F.avg(F.when(F.col("segment") == "Corporate", F.col("credit_limit"))).alias("corporate_avg_credit_limit"),
    )
    .filter(F.col("corporate_avg_credit_limit") < 1000000)
    .orderBy("credit_limit_tier")
)
