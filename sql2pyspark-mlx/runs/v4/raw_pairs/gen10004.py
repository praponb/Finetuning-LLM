from pyspark.sql import functions as F

not_flagged = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.end_date") < F.col("s.signup_date")), "left_anti")
)

result = (
    not_flagged
    .groupBy("segment")
    .agg(
        F.sum(F.when(F.col("segment") == "SMB", F.col("credit_limit")).otherwise(0)).alias("smb_credit_limit"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
    )
)
