from pyspark.sql import functions as F

valid_payments = df_payments.filter(F.col("amount") > 540)

agg = (
    valid_payments
    .groupBy("method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    agg
    .select("method", "method_values")
    .orderBy("method")
)
