from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("signup_date").desc(), "id")
w2 = Window.partitionBy("city", "account_type").orderBy(F.col("signup_date").desc_nulls_first(), "id")

cleaned = (
    df_accounts
    .filter(F.col("opened_date") < "2024-02-19")
    .withColumn("status_group", F.when(F.col("status").isin(["CLOSED", "FROZEN"]), "Tier 1").otherwise("Other"))
)

joined = (
    cleaned
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.credit_limit"), F.col("b.city"), F.col("b.signup_date"))
)

deduped = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .groupBy("status")
    .agg(
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
    )
)
