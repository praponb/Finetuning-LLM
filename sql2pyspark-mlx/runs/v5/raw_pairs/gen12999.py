from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("supplier_id").isin([13, 34]))
    .select("*", F.coalesce("brand", F.lit("unassigned")).alias("brand_filled"))
)

result = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.launch_date")), "left_semi")
)
