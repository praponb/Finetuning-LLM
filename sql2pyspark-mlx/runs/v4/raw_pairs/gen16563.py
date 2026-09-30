from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_accounts
    .filter(F.year("opened_date") >= 2023)
    .select(
        "*",
        F.concat_ws("-", "account_type", "status").alias("account_type_status_key"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("resolved_status"),
    )
)

metrics = (
    valid
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w))
    .withColumn("seq_no", F.row_number().over(w_grp))
    .withColumn("cumulative_balance", F.sum("balance").over(w3))
    .withColumn("balance_avg_2", F.avg("balance").over(w4))
)

with_dims = (
    metrics
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.credit_limit"))
)

remaining_keys = (
    with_dims
    .select("status", "id")
    .exceptAll(
        df_accounts
        .filter(~F.col("status").isin(["OPEN", "FROZEN", "CLOSED"]))
        .select("status", "id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("balance") >= 67000, "LARGE").otherwise("SMALL").alias("balance_tier"),
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
    )
    .agg(
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.percentile_approx("cumulative_balance", 0.5).alias("cumulative_balance_p50"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
    )
    .orderBy(F.col("balance_tier").asc_nulls_last())
)
