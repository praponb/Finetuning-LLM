from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("is_flagged") & F.col("account_id").isin([8, 28]) & F.col("merchant").isin(["Starbucks", "Grab"]))
)

is_flagged_summary = (
    base
    .groupBy("is_flagged")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
        F.count("*").alias("cnt"),
    )
)

result = (
    is_flagged_summary
    .filter(F.col("cnt") > 4)
    .orderBy("cnt", "txn_type_values")
)
