from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity_whole").desc())
w2 = Window.orderBy(F.col("last_restock").desc(), "id")
w3 = Window.orderBy(F.col("seq_no").desc_nulls_last())
w_grp = Window.orderBy("quantity_whole_dense_rank")
w5 = Window.orderBy(F.col("quantity_per_quantity_whole").asc_nulls_first())

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-07-18").cast("date"))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.ceil("quantity").alias("quantity_whole"),
    )
)

filtered = (
    valid
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-09-27")))
    .select(
        "id",
        "quantity",
        "last_restock",
        "quantity_whole",
        (F.col("quantity") / F.nullif("quantity_whole", F.lit(0))).alias("quantity_per_quantity_whole"),
    )
)

metrics = (
    filtered
    .filter(F.col("quantity_whole").between(24, 27))
    .select(
        "id",
        "quantity",
        "last_restock",
        "quantity_per_quantity_whole",
        F.dense_rank().over(w).alias("quantity_whole_dense_rank"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

remaining_keys = (
    metrics
    .select("id")
    .subtract(
        df_inventory
        .filter(F.month("last_restock").isin([4, 5, 12]))
        .select("id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("seq_no_dense_rank", F.dense_rank().over(w3))
    .withColumn("quantity_whole_dense_rank_percent_rank", F.percent_rank().over(w_grp))
    .withColumn("quantity_per_quantity_whole_rank", F.rank().over(w5))
)
