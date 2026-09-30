from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("balance", "id")
w2 = Window.partitionBy("country").orderBy("adjusted_prev_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("event_seq").desc())
w4 = Window.partitionBy("country").orderBy("next_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull() & (F.col("opened_date") >= F.lit("2023-04-19").cast("date")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.date_sub("opened_date", 27).alias("opened_date_minus_27d"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

enriched = (
    cleaned
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.account_type",
        "a.balance",
        "a.status_title",
        F.col("d.email"),
        F.col("d.country"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "balance",
        "email",
        "country",
        F.lag("balance").over(w).alias("prev_balance"),
        F.lead("balance").over(w).alias("next_balance"),
        F.row_number().over(w).alias("event_seq"),
    )
)

filtered = (
    metrics
    .filter(F.col("country").isNotNull() & F.col("prev_balance").between(36000, 38000))
    .withColumn("email_user", F.split(F.lower("email"), "@").getItem(0))
    .withColumn("adjusted_prev_balance", F.round((F.col("prev_balance") * 1.07), 2))
    .withColumn("country_filled", F.coalesce("country", F.lit("N/A")))
)

metrics_accounts = (
    filtered
    .filter(F.col("balance").between(37000, 40000))
    .select(
        "id",
        "balance",
        "email",
        "country",
        "event_seq",
        "country_filled",
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country"),
        F.row_number().over(w).alias("row_seq"),
        F.rank().over(w3).alias("event_seq_rank"),
        F.last("next_balance", ignorenulls=True).over(w4).alias("last_known_next_balance"),
        F.lag("next_balance", 1).over(w).alias("prev_next_balance"),
    )
)

result = (
    metrics_accounts
    .groupBy("country")
    .agg(
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.sum(F.when(F.col("country") == "US", F.col("prev_next_balance"))).alias("us_total_prev_next_balance"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("num_rows_2"),
        F.countDistinct("country").alias("distinct_countries"),
    )
    .filter(F.col("distinct_countries") < 1000)
    .orderBy("num_rows")
)
