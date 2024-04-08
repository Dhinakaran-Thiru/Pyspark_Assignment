from Pyspark_Assignment.src.Assignment_5.util import *


spark = spark_session()


employee_df = create_df(spark, employee_schema, employee_data)  # Create employee DataFrame
department_df = create_df(spark, department_schema, department_data)  # Create department DataFrame
country_df = create_df(spark, country_schema, country_data)  # Create country DataFrame



# Calculate the average salary for each department and display the results
avg_salary = find_avg_salary_employee(employee_df)  # Calculate average salary per department
avg_salary.show()  # Display average salary results

# Filter employees whose names start with 'm', join with department DataFrame to get department names, and display
employees_starts_with_m = find_employee_name_starts_with_m(employee_df, department_df)  # Filter and join with department
employees_starts_with_m.show()  # Display filtered results

# Add a new column 'bonus' to employee_df by multiplying salary by 2
employee_bonus_df = add_bonus_times_2(employee_df)  # Create bonus column
employee_bonus_df.show()  # Display DataFrame with bonus

# Reorder the columns of employee_df
rearranged_employee_df = rearrange_columns_employee_df(employee_df)  # Reorder according to specified order
rearranged_employee_df.show()  # Display DataFrame with new column order

# Perform various joins dynamically
inner_join_result = dynamic_join(employee_df, department_df, "inner")  # Perform inner join
inner_join_result.show()  # Display inner join results
left_join_result = dynamic_join(employee_df, department_df, "left")  # Perform left join
left_join_result.show()  # Display left join results
right_join_result = dynamic_join(employee_df, department_df, "right")
right_join_result.show()  # Display right join results


updated_employee_df = update_country_name(employee_df)
updated_employee_df.show()


lower_case_column_df = column_to_lower(updated_employee_df)
date_df = lower_case_column_df.withColumn("load_date", current_date())
date_df.show()