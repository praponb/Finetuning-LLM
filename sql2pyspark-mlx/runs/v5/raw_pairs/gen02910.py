from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")
w_grp = Window.partitionBy("customer_id").orderBy("adjusted_balance", "id")
w3 = Window.partitionBy("customer_id")
w_grp_2 = Window.orderBy("adjusted_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "balance",
        F.concat("account_type", F.lit("/"), "status").alias("account_type_status"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

base = (
    deduped
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "balance",
        "resolved_status",
        F.when(F.col("balance") >= 61000, "High").when(F.col("balance") >= 34000, "Medium").otherwise("Low").alias("balance_band"),
        F.round((F.col("balance") * 1.15), 2).alias("adjusted_balance"),
    )
)

scored = (
    base
    .filter(F.col("adjusted_balance").between(56, 71))
    .withColumn("seq_no", F.row_number().over(w_grp))
    .withColumn("customer_id_sum_balance", F.sum("balance").over(w3))
    .withColumn("next_adjusted_balance", F.lead("adjusted_balance", 1).over(w_grp))
    .withColumn("first_known_balance", F.first("balance", ignorenulls=True).over(w_grp_2))
)

cleaned_accounts = (
    scored
    .filter(F.col("first_known_balance").between(60000, 62000))
    .select(
        "id",
        "customer_id",
        "balance",
        "balance_band",
        "adjusted_balance",
        "customer_id_sum_balance",
        "next_adjusted_balance",
        "first_known_balance",
        F.greatest("first_known_balance", "adjusted_balance").alias("greatest_first_known_balance_adjusted_balance"),
        (F.col("customer_id_sum_balance") * F.col("balance")).alias("customer_id_sum_balance_times_balance"),
    )
)

kept_keys = (
    cleaned_accounts
    .select("id")
    .intersect(
        df_accounts
        .filter(F.col("customer_id") == 10)
        .select("id")
    )
)

verified = (
    cleaned_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.min("balance").alias("min_balance"),
        F.percentile_approx("next_adjusted_balance", 0.95).alias("next_adjusted_balance_p95"),
        F.percentile_approx("next_adjusted_balance", 0.95).alias("next_adjusted_balance_p95_2"),
        F.round(F.avg("customer_id_sum_balance_times_balance"), 2).alias("avg_customer_id_sum_balance_times_balance"),
        F.percentile_approx("greatest_first_known_balance_adjusted_balance", 0.5).alias("greatest_first_known_balance_adjusted_balance_p50"),
    )
)
