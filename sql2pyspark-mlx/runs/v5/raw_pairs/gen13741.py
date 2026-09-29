from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last())
w3 = Window.partitionBy("region").orderBy(F.col("product_id_count_quantity").asc_nulls_first())
w4 = Window.partitionBy("region")
w5 = Window.partitionBy("region").orderBy(F.col("sqft").asc_nulls_first())
w6 = Window.partitionBy("store_id")

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-07-01")))
    .withColumn("last_restock_day", F.dayofmonth("last_restock"))
    .withColumn("quantity_str", F.col("quantity").cast("string"))
)

remaining_keys = (
    filtered
    .select("store_id", "id")
    .intersect(
        df_inventory
        .filter(F.col("last_restock").between("2023-07-20", "2025-08-01"))
        .select("store_id", "id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "store_id",
        "quantity",
        "last_restock_day",
        "quantity_str",
        F.count("quantity").over(w).alias("product_id_count_quantity"),
        F.percent_rank().over(w2).alias("quantity_percent_rank"),
    )
)

enriched = (
    with_windows
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.region"), F.col("p.sqft"), F.col("p.store_name"))
)

with_windows_inventory = (
    enriched
    .select(
        "last_restock_day",
        "product_id_count_quantity",
        "region",
        F.cume_dist().over(w3).alias("product_id_count_quantity_cume_dist"),
        F.round((F.col("product_id_count_quantity") / F.sum("product_id_count_quantity").over(w4)), 4).alias("product_id_count_quantity_share"),
        F.cume_dist().over(w5).alias("sqft_cume_dist"),
        F.round((F.col("sqft") / F.sum("sqft").over(w6)), 4).alias("sqft_share"),
    )
)

result = (
    with_windows_inventory
    .select(
        "region",
        F.col("sqft_share").alias("SQFT_SHARE"),
        F.col("sqft_cume_dist").alias("SQFT_CUME_DIST"),
        "product_id_count_quantity_share",
        F.col("product_id_count_quantity_cume_dist").alias("PRODUCT_ID_COUNT_QUANTITY_CUME_DIST"),
    )
    .unpivot(
        ids=["region"],
        values=["SQFT_SHARE", "SQFT_CUME_DIST", "product_id_count_quantity_share", "PRODUCT_ID_COUNT_QUANTITY_CUME_DIST"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
