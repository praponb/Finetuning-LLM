from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("rating", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("country", "city").orderBy(F.col("next_rating").desc_nulls_first(), "id")

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.price").between(460, 770)), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category") == "Fashion"), "left_semi")
)

metrics = (
    eligible
    .select(
        "*",
        F.sum("rating").over(w).alias("cumulative_rating"),
        F.first("rating", ignorenulls=True).over(w_ordered).alias("first_known_rating"),
        F.lead("rating", 1).over(w3).alias("next_rating"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"), F.col("c.stock_qty").alias("product_stock_qty"))
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand") == "Initech"), "left_anti")
)

deduped = (
    not_flagged
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
        F.count("*").alias("cnt"),
        F.round(F.sum("next_rating") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("next_rating_per_london"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.min("next_rating").alias("min_next_rating"),
        F.countDistinct("country").alias("distinct_countries"),
    )
    .orderBy(F.col("next_rating_per_london").asc_nulls_last())
)
