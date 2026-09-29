from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .filter(F.col("balance") >= 32000)
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("account_type")).alias("account_type_list"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.signup_date"), F.col("C.account_type_list"))
)
