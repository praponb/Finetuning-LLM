from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("credit_limit", "id")

ranked = (
    df_customers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") < F.col("s.signup_date")), "left_anti")
)

remaining_keys = (
    eligible
    .select("id")
    .exceptAll(
        df_customers
        .filter(F.col("age") > 54)
        .select("id")
    )
)

kept_rows = (
    eligible
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("name").isNotNull())
    .withColumn("email_domain", F.regexp_extract("email", "@(.+)$", 1))
    .withColumn("city_title", F.initcap(F.trim("city")))
)

result = (
    base
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("activity_month"), "segment")
    .agg(
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
        F.countDistinct("segment").alias("distinct_segments"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
    )
    .filter(F.col("distinct_segments") > 1)
)
