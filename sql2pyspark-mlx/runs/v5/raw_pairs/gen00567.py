from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("credit_limit")
w2 = Window.partitionBy("city")

base = (
    df_customers
    .filter((~F.col("segment").eqNullSafe(F.lit("SMB"))) & F.col("segment").isin(["SMB", "Consumer"]))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

base_customers = (
    base
    .filter(~F.col("city").eqNullSafe(F.lit("Singapore")))
    .withColumn("email_title", F.initcap(F.trim("email")))
    .withColumn("resolved_city", F.coalesce("city", F.lit("OTHER")))
)

eligible = (
    base_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.status") != "ACTIVE"), "left_semi")
)

matched_keys = (
    eligible
    .select("country")
    .intersectAll(
        df_customers
        .filter(~F.col("city").isin(["Tokyo", "Chiang Mai"]))
        .select("country")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w))
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w2))
)

filtered = (
    metrics
    .filter((F.col("signup_date") >= F.lit("2023-08-18").cast("date")) & (F.col("country") != "JP"))
    .select(
        "*",
        F.when(F.col("country").isin(["UK", "JP"]), "Group A").otherwise("Other").alias("country_group"),
    )
)

result = (
    filtered
    .select(
        "id",
        "email",
        "country",
        "city",
        "signup_date",
        "credit_limit",
        "age",
        "is_active_label",
        "email_title",
        F.when(F.col("city").rlike("^(Tokyo|London)$"), "CORE").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("country").asc_nulls_last())
)
