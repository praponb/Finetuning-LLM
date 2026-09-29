from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("stock_qty") >= 412, "LARGE").otherwise("SMALL").alias("stock_qty_tier"),
        F.date_trunc("MONTH", "launch_date").alias("activity_month"),
    )
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.count("*").alias("cnt"),
    )
)
