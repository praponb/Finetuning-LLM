from pyspark.sql import functions as F

src = df_products.filter((F.col("cost") >= 570) | (F.col("stock_qty") == 243))

agg = (
    src
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
        "brand",
    )
    .agg(
        F.sum(F.when(F.col("brand") == "Acme", F.col("cost")).otherwise(0)).alias("acme_cost"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
    )
)

result = agg
