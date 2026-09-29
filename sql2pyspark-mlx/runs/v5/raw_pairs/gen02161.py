from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc_nulls_last(), "id")
w2 = Window.partitionBy("category").orderBy(F.col("cost").desc_nulls_last(), "id")
w3 = Window.partitionBy("category", "supplier_id").orderBy("cost", "id")
w_ordered = Window.partitionBy("category", "supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("supplier_id").orderBy(F.col("rank_in_group").desc(), "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

deduped = (
    df_products
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        "rank_in_group",
    )
)

scored = (
    latest
    .withColumn("prev_cost", F.lag("cost", 1, 0).over(w3))
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w5))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.product_id").isin([2, 7, 9])), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.cost",
        "s.launch_date",
        "s.tags",
        "s.prev_cost",
        "s.first_known_cost",
        F.col("c.last_restock"),
        F.col("c.quantity"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "product_name",
        "cost",
        "launch_date",
        "tags",
        "prev_cost",
        "quantity",
    )
    .orderBy(F.col("id").desc())
)
