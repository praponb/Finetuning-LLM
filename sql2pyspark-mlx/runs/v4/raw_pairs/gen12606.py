from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(~F.col("supplier_name").like("Gl%") & (F.col("country") == "US"))
)

country_summary = (
    src
    .groupBy("country")
    .agg((F.max("rating") - F.min("rating")).alias("rating_range"))
)

result = (
    country_summary
    .filter(F.col("rating_range") >= 76)
    .select("country", "rating_range")
)
