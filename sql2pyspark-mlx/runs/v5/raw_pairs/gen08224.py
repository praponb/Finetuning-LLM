from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.trim("supplier_name").alias("clean_supplier_name"),
        F.round("rating", 2).alias("rating_rounded"),
    )
)

result = (
    cleaned
    .select("id", "city", F.col("rating_rounded").alias("RATING_ROUNDED_VAL"), "rating")
    .unpivot(
        ids=["id", "city"],
        values=["RATING_ROUNDED_VAL", "rating"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
)
