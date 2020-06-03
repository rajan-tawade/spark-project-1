# spark-project-1
requirement

we need to join below 2 dataframe and flatten the result(1 big raw) using spark. 
the expected output also given below


df1
+------+--------+
|emp_id|emp_name|
+------+--------+
|     1|Jhone KT|
|     2|Rajan MT|
+------+--------+

df2
+------+------------+-------------+------+
|emp_id|address_type| address_line|  city|
+------+------------+-------------+------+
|     1|        home|700 hedge cox|    NJ|
|     1|     company|601 waterfall|    NJ|
|     2|        home|   888 GBroad| Thane|
|     2|     company| 999 fountain|Mumbai|
+------+------------+-------------+------+

df3
+------+--------+-----------------+---------+--------------------+------------+
|emp_id|emp_name|home_address_line|home_city|company_address_line|company_city|
+------+--------+-----------------+---------+--------------------+------------+
|     1|Jhone KT|    700 hedge cox|       NJ|       601 waterfall|          NJ|
|     2|Rajan MT|       888 GBroad|    Thane|        999 fountain|      Mumbai|
+------+--------+-----------------+---------+--------------------+------------+

