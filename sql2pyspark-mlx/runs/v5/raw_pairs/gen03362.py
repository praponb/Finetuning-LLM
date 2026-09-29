from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .filter(F.col("balance").isNull())
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.max("opened_date").alias("latest_opened_date"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(
        F.col("c.age"),
        F.col("c.credit_limit"),
        F.col("C.account_type_values"),
        F.col("C.latest_opened_date"),
    )
)
