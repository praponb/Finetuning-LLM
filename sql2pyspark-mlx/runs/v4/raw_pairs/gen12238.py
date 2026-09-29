from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")

cleaned = (
    df_accounts
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-05-14")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
        F.concat("account_type", F.lit(" "), "status").alias("account_type_status"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

metrics = (
    cleaned
    .withColumn("customer_id_max_balance", F.max("balance").over(w))
    .withColumn("customer_id_max_balance_2", F.max("balance").over(w))
)

joined = (
    metrics
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.city"))
)

grouped = (
    joined
    .groupBy("city")
    .agg(
        F.percentile_approx("customer_id_max_balance", [0.25, 0.5, 0.75]).alias("customer_id_max_balance_quartiles"),
        F.max(F.when(F.col("account_type") == "LOAN", F.col("customer_id_max_balance_2"))).alias("loan_max_customer_id_max_balance_2"),
        F.percentile_approx("customer_id_max_balance_2", [0.25, 0.5, 0.75]).alias("customer_id_max_balance_2_quartiles"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.count_if(F.col("city") == "Singapore").alias("singapore_events"),
    )
)

result = (
    grouped
    .select(
        "city",
        "customer_id_max_balance_quartiles",
        "loan_max_customer_id_max_balance_2",
        F.when(F.col("city").rlike("^(New York|Tokyo)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("city_zone"),
    )
)
