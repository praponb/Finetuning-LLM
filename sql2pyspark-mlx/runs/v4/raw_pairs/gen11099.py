from pyspark.sql import functions as F

result = df_employees.agg(F.sort_array(F.collect_set("job_title")).alias("job_title_list"))
