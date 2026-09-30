from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc(), "id")
w_part = Window.partitionBy("brand").orderBy("stock_qty", "id")

filtered = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-04-23").cast("date"))
    .select(
        "*",
        F.ceil("cost").alias("cost_whole"),
        F.concat("product_name", F.lit(" - "), "brand").alias("product_name_brand"),
    )
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

kept = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.revenue").between(1400, 3100)), "left_semi")
)

joined = (
    kept
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "inner")
    .filter(F.col("b.rating").between(1.9, 4.0))
    .select("e.*", F.col("b.country"), F.col("b.rating"))
)

enriched = (
    joined
    .select(
        "*",
        F.lead("price").over(w_part).alias("next_price"),
        F.lead("rating").over(w_part).alias("next_rating"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

result = (
    enriched
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["city", "brand"], ["city"], []],
        F.col("d.city").alias("city"),
        F.col("src.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("src.next_rating")), 2).alias("avg_next_rating"),
        F.max(F.col("src.seq_no")).alias("max_seq_no"),
        F.grouping_id().alias("gid"),
    )
)
