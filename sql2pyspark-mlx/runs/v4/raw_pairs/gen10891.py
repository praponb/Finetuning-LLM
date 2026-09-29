from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .groupBy(
        F.month("opened_date").alias("opened_month"),
        F.when(F.col("account_type").isin(["LOAN", "CREDIT"]), "group_a").otherwise("other").alias("account_type_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
    .orderBy(F.col("account_type_group").desc())
)
