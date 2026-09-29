from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("account_type").orderBy("status_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("customer_id").orderBy(F.col("status_length").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("running_count_status_length", "id")
w4 = Window.partitionBy("account_type").orderBy("status_length", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_accounts
    .filter((F.col("status").eqNullSafe(F.lit("CLOSED"))) & (F.col("balance") > 37000))
    .select("*", F.length("status").alias("status_length"))
)

scored = (
    base
    .filter(F.col("customer_id").isNull())
    .select(
        "*",
        F.sum("status_length").over(w_grp).alias("running_sum_status_length"),
        F.min("status_length").over(w_grp).alias("running_min_status_length"),
        F.count("status_length").over(w_grp).alias("running_count_status_length"),
        F.ntile(10).over(w_hist).alias("status_length_bucket"),
    )
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") >= F.col("s.opened_date")), "left_semi")
)

scored_accounts = (
    kept
    .select(
        "id",
        "opened_date",
        "status_length",
        "running_sum_status_length",
        "status_length_bucket",
        F.sum("status_length").over(w4).alias("last7_sum_status_length"),
        F.avg("status_length").over(w4).alias("last7_avg_status_length"),
        F.min("status_length").over(w4).alias("last7_min_status_length"),
    )
)

report = (
    scored_accounts
    .select(
        "id",
        "opened_date",
        "status_length",
        "running_sum_status_length",
        "status_length_bucket",
        "last7_avg_status_length",
        "last7_min_status_length",
    )
    .orderBy(F.col("id").desc_nulls_first())
)

result = report.select("id", "opened_date", "last7_min_status_length")
