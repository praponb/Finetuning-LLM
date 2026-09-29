from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id")
w2 = Window.partitionBy("customer_id").orderBy("abs_balance", "id")
w3 = Window.partitionBy("account_type").orderBy("credit_limit")
w4 = Window.partitionBy("status").orderBy("abs_balance", "id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("abs_balance").desc(), "id")

cleaned = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("opened_date") >= F.lit("2023-04-18").cast("date")))
    .select(
        "*",
        F.abs("balance").alias("abs_balance"),
        F.coalesce("status", F.lit("OTHER")).alias("status_filled"),
    )
)

joined = (
    cleaned
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select("e.*", F.col("c.name"), F.col("c.credit_limit"))
)

with_dims = (
    joined
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "a.*",
        F.col("p.signup_date"),
        F.col("p.email"),
        F.col("p.name").alias("customer_name"),
    )
)

metrics = (
    with_dims
    .select(
        "id",
        "customer_id",
        "opened_date",
        "abs_balance",
        "status_filled",
        "credit_limit",
        F.ntile(4).over(w).alias("balance_bucket"),
        F.ntile(4).over(w2).alias("abs_balance_bucket"),
        F.percent_rank().over(w3).alias("credit_limit_percent_rank"),
        F.ntile(10).over(w4).alias("abs_balance_bucket_2"),
        F.lead("abs_balance", 2, 0).over(w4).alias("next_abs_balance"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "opened_date",
        "balance_bucket",
        "abs_balance_bucket",
        "credit_limit_percent_rank",
        "latest_rank",
    )
)
