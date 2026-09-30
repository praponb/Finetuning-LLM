from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "category",
        F.col("cost").alias("COST_VAL"),
        F.col("price").alias("PRICE_VAL"),
    )
    .unpivot(["id", "category"], ["COST_VAL", "PRICE_VAL"], "kpi", "reading")
    .filter(F.col("reading").isNotNull())
)
