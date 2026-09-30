from pyspark.sql import functions as F

kept = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") >= F.col("s.opened_date")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        F.when(F.col("city").rlike("^(New York|Berlin)$"), "GROUP_B").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").asc_nulls_first())
)
