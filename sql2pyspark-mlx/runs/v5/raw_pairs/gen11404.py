from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category", "brand").orderBy(F.col("running_sum_launch_date_dayofweek").asc_nulls_last())

valid = (
    df_products
    .filter(F.col("stock_qty") <= 146)
    .select(
        "*",
        F.trim("category").alias("clean_category"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

filtered = (
    valid
    .filter(F.col("brand").isNotNull())
    .withColumn("launch_date_dayofweek", F.dayofweek("launch_date"))
    .withColumn("product_name_slug", F.regexp_replace("product_name", " ", "_"))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("N/A")))
)

scored = (
    filtered
    .filter(F.col("category") == "Fashion")
    .withColumn("running_min_launch_date_dayofweek", F.min("launch_date_dayofweek").over(w))
    .withColumn("running_sum_launch_date_dayofweek", F.sum("launch_date_dayofweek").over(w))
    .withColumn("running_count_launch_date_dayofweek", F.count("launch_date_dayofweek").over(w))
)

ranked = (
    scored
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["country", "brand"], ["country"], []],
        F.col("d.country").alias("country"),
        F.col("a.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("a.running_count_launch_date_dayofweek")).alias("total_running_count_launch_date_dayofweek"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("brand").asc_nulls_first())
)
