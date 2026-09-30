from pyspark.sql import functions as F

result = (
    df_accounts
    .rollup("customer_id")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.grouping("customer_id").alias("grouping_level"),
    )
    .orderBy(F.col("customer_id").asc_nulls_first())
)
