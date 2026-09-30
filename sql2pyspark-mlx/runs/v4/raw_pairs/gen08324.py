from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("code"))

result = (
    exploded
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .rollup("activity_day", "code")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("created_at").alias("ended_at"),
        F.count("*").alias("row_count"),
        F.grouping("activity_day").alias("grouping_level"),
    )
    .orderBy("row_count")
)
