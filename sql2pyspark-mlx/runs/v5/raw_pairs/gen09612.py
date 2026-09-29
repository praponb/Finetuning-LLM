from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("product_name").isNotNull() & (~F.col("brand").eqNullSafe(F.lit("Acme"))))
    .select(
        "*",
        (F.col("cost") * F.col("price")).alias("margin"),
        F.initcap(F.trim("brand")).alias("brand_title"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("activity_month"))
    .agg(
        F.corr("price", "cost").alias("corr_price_cost"),
        F.max(F.when(F.col("brand") == "Umbrella", F.col("stock_qty"))).alias("umbrella_max_stock_qty"),
        F.count_if(F.col("brand") == "Initech").alias("initech_events"),
    )
    .orderBy("initech_events")
)
