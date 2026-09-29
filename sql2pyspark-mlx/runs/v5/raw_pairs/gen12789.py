from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("country").isin(["UK", "DE"]) & F.col("rating").isNull() & (F.col("country") == "TH"))
)

suppliers_stats = (
    base
    .groupBy("country", "city")
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("rating")).otherwise(0)).alias("jp_rating"),
        F.sum(F.when(F.col("country") == "TH", 1).otherwise(0)).alias("th_count"),
    )
)

result = (
    suppliers_stats
    .filter(F.col("jp_rating") > 70)
    .select("country", "city", "jp_rating", "th_count")
    .orderBy("country")
)
