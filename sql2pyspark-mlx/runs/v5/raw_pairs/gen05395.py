from pyspark.sql import functions as F

rollup_totals = (
    df_orders
    .alias("a")
    .join(df_stores.alias("dim"), F.col("a.store_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["region", "city"], ["region"], []],
        F.col("dim.region").alias("region"),
        F.col("dim.city").alias("city"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("a.shipping_fee")).alias("avg_shipping_fee"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    rollup_totals
    .select(
        "region",
        "city",
        "rows_total",
        "avg_shipping_fee",
        "grouping_level",
        F.when(F.col("city").rlike("^(Tokyo|Berlin)$"), "CORE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy(F.col("region").desc_nulls_last())
)
