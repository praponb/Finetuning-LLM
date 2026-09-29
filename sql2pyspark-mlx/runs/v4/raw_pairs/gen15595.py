from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity_str").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("quantity_str", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("last_restock_minus_76d", "id")

base = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-09-10")))
    .select(
        "id",
        "store_id",
        "last_restock",
        F.date_sub("last_restock", 76).alias("last_restock_minus_76d"),
        F.col("quantity").cast("double").alias("quantity_str"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.first("quantity_str", ignorenulls=True).over(w2).alias("first_known_quantity_str"),
        F.lead("quantity_str", 1).over(w_ordered).alias("next_quantity_str"),
    )
)

joined = (
    with_windows
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.store_name"), F.col("b.region"))
)

enriched = (
    joined
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.city"), F.col("b.opened_date"))
)

result = (
    enriched
    .select(
        "id",
        "city",
        F.col("quantity_str").alias("QUANTITY_STR_VAL"),
        "next_quantity_str",
    )
    .unpivot(
        ["id", "city"],
        ["QUANTITY_STR_VAL", "next_quantity_str"],
        "metric",
        "value",
    )
)
