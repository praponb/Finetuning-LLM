from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("segment").orderBy(F.col("age").desc_nulls_first())
w3 = Window.partitionBy("country")

kept = (
    df_customers
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") < F.col("s.signup_date")), "left_anti")
)

valid = (
    kept
    .filter((F.col("signup_date") > F.to_date(F.lit("2024-04-15"))) & F.col("segment").isNotNull())
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
        F.lower("country").alias("country_lower"),
        F.coalesce("segment", F.lit("DIRECT")).alias("segment_clean"),
    )
)

matched_keys = (
    valid
    .select("city", "id")
    .subtract(
        df_customers
        .filter(F.col("is_active"))
        .select("city", "id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_customers = (
    reconciled
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") < F.col("s.signup_date")), "left_anti")
)

with_windows = (
    kept_customers
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w)), 4))
    .withColumn("age_rank", F.rank().over(w2))
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w))
    .withColumn("corr_credit_limit_age", F.corr("credit_limit", "age").over(w3))
)

result = (
    with_windows
    .select("city", "credit_limit", "age_share")
    .unpivot(["city"], ["credit_limit", "age_share"], "kpi", "val")
    .filter(F.col("val").isNotNull())
)
