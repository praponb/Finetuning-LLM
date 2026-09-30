from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.col("s.region"),
        F.when(F.year(F.col("s.opened_date")) < 2024, "before_2024").when(F.year(F.col("s.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.count(F.col("o.discount")).alias("discount_count"),
        F.max(F.col("o.shipping_fee")).alias("max_shipping_fee"),
    )
    .orderBy("region", "opened_cohort")
    .limit(1)
)
