from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-08-26"))) & F.col("supplier_id").isNotNull())
    .withColumn("launch_date_period", F.date_trunc("month", "launch_date"))
)

result = (
    base
    .select(
        "id",
        "brand",
        F.col("price").alias("PRICE_VAL"),
        F.col("cost").alias("COST_VAL"),
    )
    .unpivot(["id", "brand"], ["PRICE_VAL", "COST_VAL"], "kpi", "metric_value")
)
