from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("status").isin(["CLOSED", "FROZEN"]), "group_a").otherwise("other").alias("status_group"),
        "status",
    )
    .agg(
        F.countDistinct("balance").alias("unique_customers"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("unique_customers") < 1)
)
