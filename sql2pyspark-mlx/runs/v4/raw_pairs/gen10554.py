from pyspark.sql import functions as F

not_flagged = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") >= F.col("s.opened_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.when(F.col("store_name").rlike("^(Riverside|Mall Central)$"), "CORE").otherwise("OTHER_ZONE").alias("store_name_zone"),
    )
    .orderBy(F.col("city").asc_nulls_first(), "opened_date")
)
