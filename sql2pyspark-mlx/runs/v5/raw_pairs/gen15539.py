from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy(F.col("end_date").asc_nulls_last())

valid = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-03-29").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        F.concat("plan", F.lit(" - "), "status").alias("plan_status"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.segment"), F.col("b.email"))
)

latest = (
    enriched
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "plan")
)

cleaned = (
    latest
    .filter(F.col("customer_id") == 10)
    .withColumn("plan_lower", F.lower("plan"))
    .withColumn("plan_title", F.initcap(F.trim("plan")))
)

cleaned_subscriptions = (
    cleaned
    .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") == 29))
    .withColumn("plan_lower_2", F.lower("plan"))
)

result = (
    cleaned_subscriptions
    .groupBy("customer_id")
    .agg(
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_rows"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("row_count").desc_nulls_first())
)
