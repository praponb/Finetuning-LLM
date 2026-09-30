from pyspark.sql import functions as F

not_flagged = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") >= F.col("s.opened_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "city",
        "sqft",
        F.when(F.col("city").rlike("^(Chiang Mai|Tokyo)$"), "GROUP_A").otherwise("CORE").alias("city_zone"),
    )
    .orderBy(F.col("id").desc())
)
