from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.date_trunc("QUARTER", "launch_date").alias("quarter_start"),
        F.date_trunc("MONTH", "launch_date").alias("activity_month"),
        "category",
    )
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("category") == "Grocery", F.col("cost"))).alias("grocery_total_cost"),
        F.round(F.sum("price") / F.nullif(F.count(F.when(F.col("brand") == "Acme", 1)), F.lit(0)), 2).alias("price_per_acme"),
    )
    .orderBy("price_per_acme")
)
