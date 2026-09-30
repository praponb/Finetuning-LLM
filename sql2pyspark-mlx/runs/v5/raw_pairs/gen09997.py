from pyspark.sql import functions as F

src = df_accounts.filter(F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))

summary = (
    src
    .groupBy("account_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
    )
)

result = (
    summary
    .select("account_type", "account_type_values", "pct_open")
    .orderBy(F.col("pct_open").desc(), "account_type_values", "account_type")
    .limit(20)
)
