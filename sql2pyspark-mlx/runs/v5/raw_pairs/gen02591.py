from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("name") < 7)
    .select("city", "country")
    .subtract(
        df_suppliers
        .filter(F.col("country") == "TH")
        .select("city", "country")
    )
    .orderBy("city")
)
