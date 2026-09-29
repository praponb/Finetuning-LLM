from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 1560) & F.col("method").isin(["paypal", "bank_transfer"]))
    .groupBy("method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
)
