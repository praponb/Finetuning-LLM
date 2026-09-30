from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id")
w2 = Window.orderBy("revenue", "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("next_revenue").desc())
w4 = Window.partitionBy("store_id", "product_id").orderBy("opened_date", "id")

with_windows = (
    df_store_sales
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.lead("revenue", 1).over(w2).alias("next_revenue"),
    )
)

ranked = (
    with_windows
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.opened_date"), F.col("b.city"))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.category"))
)

deduped = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        "next_revenue",
        "row_num",
        "opened_date",
        "city",
    )
)

with_dims_store_sales = (
    deduped
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.revenue",
        "e.next_revenue",
        "e.row_num",
        "e.opened_date",
        "e.city",
        F.col("p.brand"),
    )
)

result = (
    with_dims_store_sales
    .select(
        "id",
        "product_id",
        "revenue",
        "next_revenue",
        "row_num",
        "opened_date",
        "city",
    )
    .orderBy(F.col("revenue").desc_nulls_last())
)
