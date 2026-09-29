from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "country").orderBy("end_date", "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

base = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2024-01-07").cast("date")) & (F.col("start_date") > "2024-10-01"))
    .withColumn("status_lower", F.lower("status"))
    .withColumn("start_date_plus_months", F.add_months("start_date", 1))
)

with_dims = (
    base
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.city"), F.col("b.country"))
)

deduped = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

base_subscriptions = (
    deduped
    .filter((F.col("end_date") > F.lit("2023-05-09").cast("date")) & F.col("status").isin(["EXPIRED", "CANCELLED"]))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status_lower",
        "start_date_plus_months",
        "country",
        F.date_sub("end_date", 5).alias("end_date_minus_5d"),
    )
)

with_dims_subscriptions = (
    base_subscriptions
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.country").alias("customer_country"), F.col("b.name"))
)

result = (
    with_dims_subscriptions
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status_lower",
        "start_date_plus_months",
        "customer_country",
        F.when(F.col("customer_country").rlike("^(US|DE)$"), "GROUP_A").otherwise("CORE").alias("customer_country_zone"),
        F.col("max_account_balance"),
    )
    .orderBy(F.col("start_date").desc_nulls_first())
)
