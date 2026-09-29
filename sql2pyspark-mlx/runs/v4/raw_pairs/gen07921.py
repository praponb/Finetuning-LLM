from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id")
w3 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2024-03-26").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
    )
)

metrics = (
    filtered
    .withColumn("revenue_avg_6", F.avg("revenue").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("running_revenue_per_unit", F.sum("revenue_per_unit").over(w3))
)

with_dims = (
    metrics
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.sqft"), F.col("b.store_name"), F.col("b.opened_date"))
)

result = (
    with_dims
    .withColumn("revenue_avg_6_tier", F.when(F.col("revenue_avg_6") >= 3179, "HIGH").otherwise("LOW"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "sale_date"))
    .groupingSets(
        [["revenue_avg_6_tier", "quarter_start", "store_id"], ["revenue_avg_6_tier", "quarter_start"], []],
        "revenue_avg_6_tier",
        "quarter_start",
        "store_id",
    )
    .agg(
        F.bit_and("units").alias("units_bit_and"),
        F.max("row_seq").alias("max_row_seq"),
        F.percentile_approx("sqft", 0.5).alias("sqft_p50"),
        F.bit_and("units").alias("units_bit_and_2"),
        F.bit_and("units").alias("units_bit_and_3"),
        F.max("revenue_per_unit").alias("max_revenue_per_unit"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("sqft_p50").desc())
)
