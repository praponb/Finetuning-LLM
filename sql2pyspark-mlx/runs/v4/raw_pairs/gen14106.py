from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_inventory
    .select(
        "*",
        F.count("quantity").over(w).alias("product_id_count_quantity"),
        F.row_number().over(w2).alias("row_seq"),
        F.avg("quantity").over(w3).alias("quantity_avg_2_prior"),
        F.sum("quantity").over(w4).alias("cumulative_quantity"),
    )
)

result = (
    scored
    .cube(
        F.date_trunc("quarter", "last_restock").alias("quarter_start"),
        F.date_trunc("week", "last_restock").alias("week_start"),
    )
    .agg(
        F.percentile_approx("cumulative_quantity", [0.25, 0.5, 0.75]).alias("cumulative_quantity_quartiles"),
        F.percentile_approx("product_id_count_quantity", 0.5).alias("product_id_count_quantity_p50"),
        F.round(F.avg("quantity_avg_2_prior"), 2).alias("avg_quantity_avg_2_prior"),
    )
    .filter(F.col("product_id_count_quantity_p50") > 0)
)
