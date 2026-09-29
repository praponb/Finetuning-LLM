from pyspark.sql import functions as F

kept = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") >= F.col("s.opened_date")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.when(F.col("region").rlike("^(South|Central)$"), "EXTENDED").when(F.col("region").rlike("^(North|East)$"), "PRIORITY").otherwise("GROUP_A").alias("region_zone"),
    )
    .orderBy(F.col("region_zone").asc_nulls_first())
)
