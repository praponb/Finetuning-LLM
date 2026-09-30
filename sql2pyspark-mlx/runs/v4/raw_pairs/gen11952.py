from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w_grp = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("rating_share").asc_nulls_first())
w5 = Window.partitionBy("city").orderBy(F.col("row_seq").asc_nulls_first())

base = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%aci%"))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.concat_ws("-", "country", "supplier_name").alias("country_supplier_name_key"),
    )
)

enriched = (
    base
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("first_known_city", F.first("city", ignorenulls=True).over(w3))
)

with_windows = (
    enriched
    .withColumn("rating_share_cume_dist", F.cume_dist().over(w4))
    .withColumn("row_seq_cume_dist", F.cume_dist().over(w5))
)

result = (
    with_windows
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.percentile_approx("row_seq_cume_dist", 0.95).alias("row_seq_cume_dist_p95"),
    )
    .filter(F.col("num_rows") > 1)
)
