from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("country").orderBy("running_avg_age", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("account_account_type", "city").orderBy("balance", "id")
w_grp = Window.partitionBy("city").orderBy("running_count_age", "id")
w7 = Window.partitionBy("city", "account_account_type").orderBy("next_balance", "id")

eligible = (
    df_customers
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.amount") < 1000), "left_anti")
)

with_windows = (
    eligible
    .filter(F.month("signup_date").isin([1, 5, 11]))
    .withColumn("running_max_age", F.max("age").over(w_part))
    .withColumn("running_avg_age", F.avg("age").over(w_part))
    .withColumn("running_sum_age", F.sum("age").over(w_part))
    .withColumn("running_count_age", F.count("age").over(w_part))
)

with_last = (
    with_windows
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.country",
        "s.city",
        "s.signup_date",
        "s.is_active",
        "s.credit_limit",
        "s.running_avg_age",
        "s.running_sum_age",
        "s.running_count_age",
        F.col("c.account_type").alias("account_account_type"),
    )
)

latest = (
    with_last
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

with_latest = (
    latest
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.city",
        "s.signup_date",
        "s.is_active",
        "s.running_count_age",
        "s.account_account_type",
        "s.dedup_rank",
        F.col("c.account_type").alias("account_account_type_2"),
        F.col("c.balance"),
    )
)

with_windows_customers = (
    with_latest
    .select(
        "id",
        "name",
        "city",
        "is_active",
        "running_count_age",
        "account_account_type",
        "balance",
        F.ntile(3).over(w5).alias("balance_bucket"),
        F.lead("balance", 1).over(w_grp).alias("next_balance"),
    )
)

ranked = (
    with_windows_customers
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "name",
        "city",
        "is_active",
        "account_account_type",
        "balance",
        "balance_bucket",
        "next_balance",
    )
)

result = (
    ranked
    .groupBy("account_account_type")
    .pivot("city", ["Tokyo", "Chiang Mai"])
    .agg(F.max("next_balance").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "account_account_type",
        F.col("Tokyo_amount").alias("tokyo_amount"),
        F.col("Tokyo_item_count").alias("tokyo_item_count"),
        F.col("Chiang Mai_amount").alias("chiang_mai_amount"),
        F.col("Chiang Mai_item_count").alias("chiang_mai_item_count"),
    )
)
