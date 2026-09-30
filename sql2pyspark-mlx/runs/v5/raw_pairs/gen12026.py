from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("amount").desc_nulls_first(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-04-04").cast("date")) & F.col("segment").isNotNull())
    .withColumn("name_prefix", F.substring("name", 1, 2))
)

latest_match = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"), F.col("c.status"))
)

kept = (
    latest_match
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.signup_date")), "left_semi")
)

deduped = (
    kept
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "name", "country", "city", "segment", "age", "name_prefix", "amount")
)

with_latest = (
    deduped
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.age",
        F.col("c.account_type").alias("account_account_type"),
    )
)

filtered = (
    with_latest
    .filter(F.col("city").isNotNull())
    .withColumn("age_level", F.when(F.col("age") >= 60, "High").when(F.col("age") >= 41, "Medium").otherwise("Low"))
)

base = (
    filtered
    .filter(~F.col("country").isin(["TH", "JP"]))
    .select(
        "*",
        F.coalesce("account_account_type", F.lit("unassigned")).alias("account_account_type_filled"),
        F.coalesce("account_account_type", F.lit("N/A")).alias("resolved_account_account_type"),
    )
)

result = (
    base
    .groupBy("account_account_type")
    .agg(
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
    )
    .orderBy(F.col("account_account_type").desc_nulls_first())
)
