from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

result = (
    lines
    .withColumn("hour_window", F.date_trunc("HOUR", "created_at"))
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .groupingSets(
        [["hour_window", "activity_day"], ["hour_window"]],
        "hour_window",
        "activity_day",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("element")).alias("num_elements"),
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("element")).alias("element_list"),
        F.count_if(F.col("element") == "VIP").alias("vip_events"),
    )
)
