from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("*", F.col("amount").cast("int").alias("amount_int"))
)

result = (
    base
    .cube(
        F.when(F.col("amount_int") >= 75, "LARGE").otherwise("SMALL").alias("amount_int_tier"),
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.percentile_approx("amount_int", [0.25, 0.5, 0.75]).alias("amount_int_quartiles"),
        F.min("paid_at").alias("started_at"),
        F.sum(F.when(F.col("method") == "card", F.col("amount_int")).otherwise(0)).alias("card_amount_int"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("duration_min").asc_nulls_first())
)
