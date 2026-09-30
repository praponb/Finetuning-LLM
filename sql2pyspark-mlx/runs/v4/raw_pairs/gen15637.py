from pyspark.sql import functions as F

joined = (
    df_order_items
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.launch_date"), F.col("p.brand"))
)

result = (
    joined
    .groupBy(F.date_trunc("QUARTER", "launch_date").alias("quarter_start"))
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_rows"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_rows_2"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
)
