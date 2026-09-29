from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id")

base = (
    df_stores
    .filter(F.col("region").isNotNull())
    .withColumn("region_title", F.initcap(F.trim("region")))
    .withColumn("region_store_name", F.concat("region", F.lit("/"), "store_name"))
)

base_stores = (
    base
    .filter((F.col("opened_date") >= F.lit("2023-08-13").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.concat("store_name", F.lit(" - "), "city").alias("store_name_city"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

eligible = (
    base_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id").isin([1, 2, 3])), "left_anti")
)

deduped = (
    eligible
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "region_title",
        "region_store_name",
        "store_name_city",
    )
)
