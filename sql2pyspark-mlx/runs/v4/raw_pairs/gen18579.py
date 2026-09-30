from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["DE", "TH", "US"]) & F.col("rating").isNull() & F.col("supplier_name").like("E%"))
    .select(
        "id",
        "rating",
        "country",
        F.round((F.col("rating") * 1.07), 2).alias("adjusted_rating"),
    )
    .orderBy("id")
)
