from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc_nulls_last())
w2 = Window.partitionBy("region", "city").orderBy("sqft", "id")
w3 = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_stores
    .select(
        "*",
        F.rank().over(w).alias("sqft_rank"),
        F.lag("sqft", 2, 0).over(w2).alias("prev_sqft"),
        F.last("region", ignorenulls=True).over(w3).alias("last_known_region"),
        F.last("region", ignorenulls=True).over(w4).alias("last_known_region_2"),
    )
)

base = (
    scored
    .filter((F.col("opened_date") > F.lit("2023-08-09").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "sqft_rank",
        "prev_sqft",
        F.round((F.col("sqft_rank") * 100), 2).alias("sqft_rank_scaled"),
        F.concat_ws("-", "store_name", "region").alias("store_name_region_key"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") >= F.col("s.opened_date")), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft_rank",
        "prev_sqft",
        F.when(F.col("store_name").rlike("^(Mall Central|Old Town)$"), "CORE").when(F.col("store_name").rlike("^(Siam Flagship|Downtown Express)$"), "GROUP_A").otherwise("PRIORITY").alias("store_name_zone"),
    )
)
