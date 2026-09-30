from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("week_start").orderBy(F.col("age_stddev").desc())

result = (
    df_customers
    .groupBy(
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
        F.when(F.col("age") >= 66, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.stddev_samp("age").alias("age_stddev"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
    .withColumn("age_stddev_rank", F.rank().over(w))
    .orderBy(F.col("week_start").asc_nulls_first())
)
