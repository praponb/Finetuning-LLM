from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")
w4 = Window.partitionBy("account_type").orderBy(F.col("balance_rounded").desc(), "id")
w5 = Window.partitionBy("status").orderBy("balance_rounded", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("customer_id").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("customer_id", "status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-05-01").cast("date")) & (F.col("balance") > 41000))
    .withColumn("balance_rounded", F.round("balance", 0))
    .withColumn("resolved_status", F.coalesce("status", F.lit("NONE")))
)

scored = (
    base
    .filter(F.col("balance").isNotNull())
    .select(
        "*",
        F.first("status", ignorenulls=True).over(w).alias("first_known_status"),
        F.sum("balance_rounded").over(w2).alias("cumulative_balance_rounded"),
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

joined = (
    scored
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.account_type",
        "a.balance",
        "a.opened_date",
        "a.status",
        "a.balance_rounded",
        "a.cumulative_balance_rounded",
        "a.row_seq",
        "a.seq_no",
        F.col("b.age"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        "cumulative_balance_rounded",
        "row_seq",
        "seq_no",
        "age",
        F.sum("row_seq").over(w2).alias("cumulative_row_seq"),
        F.avg("seq_no").over(w5).alias("seq_no_avg_3"),
    )
)

metrics_accounts = (
    metrics
    .select(
        "id",
        "customer_id",
        "row_seq",
        "age",
        "cumulative_row_seq",
        F.last("balance", ignorenulls=True).over(w6).alias("last_known_balance"),
        F.last("seq_no_avg_3", ignorenulls=True).over(w7).alias("last_known_seq_no_avg_3"),
        F.sum("row_seq").over(w8).alias("cumulative_row_seq_2"),
    )
)

stats = (
    metrics_accounts
    .groupBy("customer_id")
    .agg(
        F.sum("last_known_balance").alias("total_last_known_balance"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("num_rows_2"),
        F.round(F.avg("cumulative_row_seq_2"), 2).alias("avg_cumulative_row_seq_2"),
        F.count("*").alias("cnt"),
    )
)

result = (
    stats
    .select(
        "customer_id",
        "total_last_known_balance",
        "num_rows",
        "num_rows_2",
        "avg_cumulative_row_seq_2",
        "cnt",
    )
    .orderBy(F.col("cnt").desc())
)
