from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-03-19"))) & F.col("customer_id").isNull())
    .withColumn("balance_tier", F.when(F.col("balance") >= 87000, "High").when(F.col("balance") >= 15000, "Medium").otherwise("Low"))
)

with_dims = (
    base
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.name"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.max("balance").over(w).alias("last7_max_balance"),
        F.sum("balance").over(w).alias("last7_sum_balance"),
        F.count("balance").over(w).alias("last7_count_balance"),
        F.avg("balance").over(w).alias("last7_avg_balance"),
    )
)

enriched = (
    with_windows
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.account_type",
        "e.opened_date",
        "e.name",
        "e.last7_max_balance",
        "e.last7_sum_balance",
        "e.last7_count_balance",
        "e.last7_avg_balance",
        F.col("c.email"),
    )
)

matched_keys = (
    enriched
    .select("customer_id", "id")
    .exceptAll(
        df_accounts
        .filter(F.col("account_type") == "CREDIT")
        .select("customer_id", "id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

stats = (
    kept_rows
    .groupBy(
        F.date_trunc("quarter", "opened_date").alias("quarter_start"),
        F.date_trunc("week", "opened_date").alias("week_start"),
    )
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.round(F.avg("last7_sum_balance"), 2).alias("avg_last7_sum_balance"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list_2"),
    )
    .filter(F.col("avg_last7_sum_balance") > 0)
)

result = (
    stats
    .select(
        "quarter_start",
        "week_start",
        "account_type_list",
        "avg_last7_sum_balance",
        "account_type_list_2",
    )
)
