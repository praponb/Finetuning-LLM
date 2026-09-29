from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy("opened_date", "id")
w3 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("balance", "id")
w5 = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")

valid = (
    df_accounts
    .filter((~F.col("status").eqNullSafe(F.lit("FROZEN"))) & (F.col("customer_id") == 5))
    .withColumn("status_account_type", F.concat("status", F.lit(" "), "account_type"))
    .withColumn("resolved_status", F.coalesce("status", F.lit("OTHER")))
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    deduped
    .withColumn("next_balance", F.lead("balance", 1).over(w2))
    .withColumn("running_balance", F.sum("balance").over(w3))
    .withColumn("next_balance_2", F.lead("balance", 2).over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
)

cleaned = (
    with_windows
    .filter((F.col("opened_date") >= F.lit("2023-03-02").cast("date")) & F.col("status").isNull())
    .withColumn("next_balance_rounded", F.round("next_balance", 2))
    .withColumn("status_filled", F.coalesce("status", F.lit("OTHER")))
)

result = (
    cleaned
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.city").alias("city"), F.col("s.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("s.seq_no")).alias("total_seq_no"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("city").asc_nulls_last())
)
