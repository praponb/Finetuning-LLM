from pyspark.sql import functions as F

src = df_suppliers.filter(~((F.col("rating") < 2.1) & (F.length("supplier_name") > 5)))

summary = (
    src
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)

result = summary.select("city", "country_values")
