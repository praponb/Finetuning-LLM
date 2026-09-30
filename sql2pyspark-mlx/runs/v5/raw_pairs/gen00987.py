from pyspark.sql import functions as F

valid = (
    df_stores
    .filter(F.col("region").isNotNull() & (~F.col("region").eqNullSafe(F.lit("Central"))))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        F.when(F.col("region").isin(["West", "South"]), "Group A").otherwise("Other").alias("region_group"),
        F.concat("store_name", F.lit(" - "), "city").alias("store_name_city"),
    )
)

result = (
    valid
    .select("city")
    .exceptAll(
        df_stores
        .filter(F.col("opened_date") >= "2025-10-09")
        .select("city")
    )
)
