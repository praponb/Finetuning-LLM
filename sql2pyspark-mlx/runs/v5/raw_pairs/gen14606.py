from pyspark.sql import functions as F

not_flagged = (
    df_stores
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") <= F.col("s.opened_date")), "left_anti")
)

result = (
    not_flagged
    .groupBy(F.date_trunc("month", "opened_date").alias("activity_month"), "region")
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.bit_and("sqft").alias("sqft_bit_and"),
    )
    .orderBy(F.col("activity_month").desc())
)
