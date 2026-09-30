from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower(F.trim("supplier_name")) == "eurogoods gmbh")
    .select("country")
    .intersect(
        df_sessions
        .filter(F.col("country").isin(["DE", "UK", "TH"]))
        .select("country")
    )
    .orderBy("country")
)
