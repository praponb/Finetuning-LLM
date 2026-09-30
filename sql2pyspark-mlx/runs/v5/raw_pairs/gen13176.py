from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_first(), "id")

with_dims = (
    df_inventory
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.quantity",
        F.col("b.category"),
        F.col("b.launch_date"),
    )
)

scored = (
    with_dims
    .select(
        "*",
        F.avg("quantity").over(w).alias("last7_avg_quantity"),
        F.count("quantity").over(w).alias("last7_count_quantity"),
        F.min("quantity").over(w).alias("last7_min_quantity"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

with_dims_inventory = (
    deduped
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.cost") > 340)
    .select(
        "a.id",
        "a.product_id",
        "a.category",
        "a.last7_avg_quantity",
        "a.last7_count_quantity",
        "a.rank_in_group",
        F.col("d.product_name"),
        F.col("d.launch_date").alias("product_launch_date"),
        F.col("d.cost"),
    )
)

valid = (
    with_dims_inventory
    .filter(F.col("cost").between(350, 710))
    .select(
        "*",
        (F.col("cost") / F.nullif("last7_avg_quantity", F.lit(0))).alias("cost_per_last7_avg_quantity"),
        F.round((F.col("last7_count_quantity") * 1.15), 2).alias("adjusted_last7_count_quantity"),
    )
)

result = (
    valid
    .cube(
        F.when(F.col("last7_avg_quantity") >= 218, "LARGE").otherwise("SMALL").alias("last7_avg_quantity_tier"),
        F.date_trunc("YEAR", "product_launch_date").alias("year_start"),
    )
    .agg(
        F.percentile_approx("last7_count_quantity", [0.25, 0.5, 0.75]).alias("last7_count_quantity_quartiles"),
        F.avg(F.when(F.col("category") == "Books", F.col("cost_per_last7_avg_quantity"))).alias("books_avg_cost_per_last7_avg_quantity"),
    )
)
