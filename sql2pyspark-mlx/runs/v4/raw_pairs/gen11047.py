from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_store_sales
    .withColumn("revenue_bucket", F.ntile(10).over(w))
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4))
    .withColumn("first_known_units", F.first("units", ignorenulls=True).over(w3))
)

result = (
    with_windows
    .rollup(
        F.when(F.col("units") >= 44, "LARGE").otherwise("SMALL").alias("units_tier"),
        F.date_trunc("YEAR", "sale_date").alias("year_start"),
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.bit_and("units").alias("units_bit_and"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("distinct_store_ids") > 1)
    .orderBy("distinct_store_ids")
)
