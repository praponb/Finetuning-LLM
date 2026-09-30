from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(580, 1470))
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy(F.col("status_values").desc())
)
