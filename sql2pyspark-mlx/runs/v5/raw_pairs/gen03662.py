from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.percentile_approx("balance", 0.5).alias("balance_p50"),
    )
)
