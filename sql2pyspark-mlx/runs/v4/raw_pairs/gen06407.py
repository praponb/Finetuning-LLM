from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "card", "bank_transfer"]))
    .groupBy(F.to_date("paid_at").alias("paid_day"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
)
