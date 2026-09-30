from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("age_tier").orderBy(F.col("mobile_avg_age").desc())

with_dims = (
    df_sessions
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.signup_date"), F.col("b.age"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("age") >= 62, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        "browser",
    )
    .agg(
        F.max("started_at").alias("last_started_at"),
        F.count_if(F.col("browser") == "Chrome").alias("chrome_events"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.avg(F.when(F.col("device") == "mobile", F.col("age"))).alias("mobile_avg_age"),
    )
    .withColumn("mobile_avg_age_dense_rank", F.dense_rank().over(w))
)
