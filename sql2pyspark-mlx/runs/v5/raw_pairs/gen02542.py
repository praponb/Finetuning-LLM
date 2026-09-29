from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-5, Window.currentRow)

filtered = (
    df_suppliers
    .filter(F.col("country").isin(["US", "SG", "TH"]))
    .select(
        "*",
        F.concat_ws(",", "country", "supplier_name").alias("country_supplier_name_key"),
        F.floor("rating").alias("rating_whole"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.cost").between(250, 600)), "left_semi")
)

enriched = (
    eligible
    .select(
        "id",
        "supplier_name",
        "country",
        "country_supplier_name_key",
        "rating_whole",
        "resolved_city",
        F.sum("rating").over(w).alias("country_sum_rating"),
        F.avg("rating_whole").over(w2).alias("rating_whole_avg_5"),
    )
)

valid = (
    enriched
    .filter(F.col("rating_whole") >= 27)
    .select(
        "*",
        F.when(F.col("country") == "SG", 1).otherwise(0).alias("is_sg"),
        F.upper("country").alias("country_upper"),
    )
)

result = (
    valid
    .groupBy(
        "country",
        F.when(F.col("rating_whole") >= 81, "PREMIUM").otherwise("STANDARD").alias("rating_whole_tier"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "US", F.col("rating_whole_avg_5"))).alias("us_total_rating_whole_avg_5"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.count("*").alias("row_count"),
    )
)
