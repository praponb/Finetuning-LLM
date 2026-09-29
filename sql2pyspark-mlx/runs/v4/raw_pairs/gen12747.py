from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-06-28").cast("date")) & F.col("brand").isNotNull())
    .withColumn("greatest_price_cost", F.greatest("price", "cost"))
    .withColumn("adjusted_price", F.round((F.col("price") * 100), 2))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("OTHER")))
)

result = (
    base
    .select(
        "id",
        "category",
        F.col("cost").alias("COST_VAL"),
        F.col("greatest_price_cost").alias("GREATEST_PRICE_COST"),
    )
    .unpivot(
        ["id", "category"],
        ["COST_VAL", "GREATEST_PRICE_COST"],
        "metric_name",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
