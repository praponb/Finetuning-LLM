from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("uk_events").desc())

kept_keys = (
    df_customers
    .select("city")
    .exceptAll(
        df_customers
        .filter(F.col("email").isNotNull())
        .select("city")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("city", F.date_trunc("week", "signup_date").alias("week_start"))
    .agg(
        F.count_if(F.col("country") == "UK").alias("uk_events"),
        F.sum(F.when(F.col("country") == "JP", F.col("age"))).alias("jp_total_age"),
    )
    .withColumn("uk_events_dense_rank", F.dense_rank().over(w))
)
