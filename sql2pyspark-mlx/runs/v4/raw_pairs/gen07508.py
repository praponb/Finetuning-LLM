from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
        F.max("txn_time").alias("last_txn_time"),
        F.countDistinct("amount").alias("unique_txn_types"),
    )
)
