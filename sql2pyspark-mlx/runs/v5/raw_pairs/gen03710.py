from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .select(
        "id",
        "country",
        F.concat("supplier_name", F.lit(", "), "city").alias("supplier_name_city"),
    )
)
