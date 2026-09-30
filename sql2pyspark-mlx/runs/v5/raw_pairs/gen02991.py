from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue", "id")
w2 = Window.partitionBy("store_id").orderBy("units", "id")

base = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4]))
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
)

scored = (
    base
    .select(
        "id",
        "store_id",
        "revenue_per_unit",
        F.ntile(5).over(w).alias("revenue_bucket"),
        F.lead("revenue", 1).over(w2).alias("next_revenue"),
    )
)

kept_keys = (
    scored
    .select("store_id", "id")
    .exceptAll(
        df_store_sales
        .filter(F.col("units").between(21, 24))
        .select("store_id", "id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.opened_date"), F.col("b.city"), F.col("b.sqft"))
)

result = (
    enriched
    .select("id", "revenue_per_unit", "revenue_bucket", "next_revenue", "sqft")
    .orderBy(F.col("sqft").desc())
)
