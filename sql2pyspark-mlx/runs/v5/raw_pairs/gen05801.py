from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy("city")

with_windows = (
    df_stores
    .filter(F.col("city") != "Tokyo")
    .select(
        "*",
        F.avg("sqft").over(w).alias("sqft_avg_2"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

remaining_keys = (
    with_windows
    .select("city", "id")
    .subtract(
        df_stores
        .filter(F.col("city") != "Bangkok")
        .select("city", "id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") < F.col("s.opened_date")), "left_anti")
)

scored = eligible.select("*", F.avg("row_seq").over(w3).alias("city_avg_row_seq"))

result = (
    scored
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "sqft_avg_2",
        F.when(F.col("region").rlike("^(South|West)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("region_zone"),
    )
)
