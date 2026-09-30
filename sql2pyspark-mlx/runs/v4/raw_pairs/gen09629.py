from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date_minus_56d").desc())
w2 = Window.partitionBy("balance_tier").orderBy(F.col("avg_balance_2").desc())

valid = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.year("opened_date") == 2024))
    .withColumn("opened_date_minus_56d", F.date_sub("opened_date", 56))
    .withColumn("status_filled", F.coalesce("status", F.lit("DIRECT")))
)

deduped = (
    valid
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

with_dims = (
    deduped
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.name"), F.col("c.signup_date"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("balance") >= 79000, "HIGH").otherwise("LOW").alias("balance_tier"),
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("balance"), 2).alias("avg_balance_2"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.count("*").alias("num_rows_2"),
    )
    .withColumn("avg_balance_2_dense_rank", F.dense_rank().over(w2))
)
