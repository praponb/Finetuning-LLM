from pyspark.sql import functions as F

by_dimension = (
    df_orders
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "inner")
    .cube(F.col("s.city").alias("city"), F.col("src.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.shipping_fee")).alias("max_shipping_fee"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    by_dimension
    .select(
        "city",
        "rows_total",
        "grouping_level",
        F.when(F.col("city").rlike("^(Tokyo|Singapore)$"), "OTHER_ZONE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_B").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy(F.col("rows_total").desc())
)
