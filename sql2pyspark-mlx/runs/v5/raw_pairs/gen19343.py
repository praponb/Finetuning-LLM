from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("prev_units").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w4 = Window.partitionBy("product_id")
w5 = Window.orderBy("seq_no_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_store_sales
    .filter(F.col("revenue") <= 2800)
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.lead("revenue").over(w_recent).alias("next_revenue"),
        F.lag("revenue").over(w_recent).alias("prev_revenue"),
        F.lag("units").over(w_recent).alias("prev_units"),
        F.lead("units").over(w_recent).alias("next_units"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "product_id",
        "revenue",
        "next_units",
        "seq_no",
        F.row_number().over(w2).alias("seq_no_2"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

scored = (
    with_windows
    .withColumn("next_units_share", F.round((F.col("next_units") / F.sum("next_units").over(w4)), 4))
    .withColumn("first_known_seq_no", F.first("seq_no", ignorenulls=True).over(w5))
)

kept_keys = (
    scored
    .select("product_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("sale_date") >= "2024-01-01")
        .select("product_id", "id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "product_id",
        "revenue",
        "seq_no_2",
        "row_seq",
        "next_units_share",
        "first_known_seq_no",
    )
)
