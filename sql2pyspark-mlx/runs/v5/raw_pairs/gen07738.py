from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-06-12"))) & F.col("brand").isNotNull())
    .select("*", F.coalesce("cost", F.lit(0)).alias("cost_filled"))
)

result = (
    base
    .select(
        "id",
        "category",
        F.col("price").alias("PRICE"),
        F.col("cost_filled").alias("COST_FILLED"),
        F.col("cost").alias("COST"),
    )
    .unpivot(
        ids=["id", "category"],
        values=["PRICE", "COST_FILLED", "COST"],
        variableColumnName="measure",
        valueColumnName="value",
    )
)
