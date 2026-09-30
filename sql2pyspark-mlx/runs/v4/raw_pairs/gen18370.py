from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id")

latest = (
    df_accounts
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.signup_date"), F.col("c.country"), F.col("c.credit_limit"))
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)

result = (
    not_flagged
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("activity_month"))
    .agg(
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.percentile_approx("credit_limit", 0.5).alias("credit_limit_p50"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
)
