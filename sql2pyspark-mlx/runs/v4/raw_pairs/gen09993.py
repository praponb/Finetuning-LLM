from pyspark.sql import functions as F

joined = (
    df_sessions
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.name"), F.col("c.credit_limit"), F.col("c.signup_date"))
)

result = (
    joined
    .groupBy("device", F.date_trunc("quarter", "started_at").alias("quarter_start"))
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.max("started_at").alias("last_started_at"),
        F.sum(F.when(F.col("device") == "desktop", F.col("credit_limit")).otherwise(0)).alias("desktop_credit_limit"),
    )
    .filter(F.col("span_minutes") >= 0)
)
