from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 1.8) & (F.col("country") != "TH"))
    .select(
        "id",
        "city",
        F.substring("supplier_name", 1, 4).alias("supplier_name_prefix"),
        F.round((F.col("rating") * 0.85), 2).alias("rating_scaled"),
    )
)
