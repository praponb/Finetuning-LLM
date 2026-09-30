from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id")
w_recent = Window.orderBy("opened_date", "id").rowsBetween(-5, -1)
w_ordered = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("opened_date", "id")

base = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2024-02-20").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        F.trim("account_type").alias("clean_account_type"),
        F.coalesce("status", F.lit("N/A")).alias("status_clean"),
    )
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    ranked
    .withColumn("balance_avg_5_prior", F.avg("balance").over(w_recent))
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w_ordered))
)

latest = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "clean_account_type",
        "status_clean",
        "balance_avg_5_prior",
        "first_known_status",
        "rank_in_group",
    )
)
