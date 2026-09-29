from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy("sale_date", "id")
w4 = Window.partitionBy("store_id").orderBy("revenue", "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")

enriched = (
    df_store_sales
    .select(
        "*",
        F.ntile(5).over(w).alias("revenue_bucket"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4).alias("revenue_share"),
        F.lead("revenue", 1).over(w3).alias("next_revenue"),
        F.row_number().over(w4).alias("event_seq"),
        F.ntile(5).over(w5).alias("units_bucket"),
    )
)

result = (
    enriched
    .groupBy(
        "product_id",
        F.date_trunc("YEAR", "sale_date").alias("year_start"),
        F.when(F.col("revenue") >= 4400, "PREMIUM").otherwise("STANDARD").alias("revenue_tier"),
    )
    .agg(
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.bit_and("units").alias("units_bit_and"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .orderBy(F.col("revenue_tier").desc())
)
