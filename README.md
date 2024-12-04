# Nuga_Bank_Scheduler

## Summary:
Nuga Bank is embarking on a data-centric transformation project to
enhance operational efficiency, improve customer service, and enable more
effective decision-making through advanced analytics. The project involves
building a robust data pipeline that extracts, transforms, and loads (ETL)
data, allowing for better data governance and analytics readiness. The case
study will cover the technical approach, business impacts, and the
technologies employed in achieving these goals.

## Objectives:
● Streamline Data Processes: Automate the extraction, transformation, and
loading of data to reduce manual intervention and errors.

● Ensure Data Quality: Implement checks and transformations that ensure the
data adheres to the highest quality standards.

● Data Normalization: Structure the data into 2nd Normal Form (2NF) or 3rd
Normal Form (3NF) to reduce redundancy and improve logical consistency.

● Improve Data Accessibility: Enable easier access to data through a
centralized PostgreSQL database, facilitating more complex queries and
analytics.

● Enhance Decision Making: Provide clean, organized, and reliable data that
can be easily analyzed to support business decisions.

## TECH STACK:
For this case study, the following tech stack was employed:
A. Python: For scripting, data manipulation, and process automation.

B. SQL: For data querying and manipulation within databases.

C. PySpark: For handling large datasets and performing data processing tasks in a
distributed environment.

D. PostgreSQL: As the relational database system to store, query, and manage the
processed data.

E. Windows Task Scheduler: For automating and scheduling ETL jobs.

F. GitHub: For version control and collaboration among the development team.

![image](https://github.com/user-attachments/assets/2ac17eeb-384e-4608-b6f6-fa42ac22a936)

## Project Scope:
# `A. Data Extraction`
● Setup and Initialize Your Spark Session: Start a PySpark session to handle distributed data processing
tasks.
● Extract a Historical CSV File into Spark DataFrame: Utilize PySpark to read historical transaction data from
CSV files, allowing for scalable data manipulation and initial preprocessing.
# `B. Data Transformation`
● Clean the Dataset: Implement data cleaning techniques such as handling missing values, standardizing
formats, and removing duplicates.
● Transform the Dataset into 2NF or 3NF: Apply normalization rules to organize data into either the Second
Normal Form or Third Normal Form to reduce redundancy and improve data integrity.

![schema](https://github.com/user-attachments/assets/c9285e25-57d2-435d-9cd7-bc3acd494dcd)


# `C. Data Loading`
● Load the Normalized Dataset into a PostgreSQL Database: Utilize SQL and PySpark's database connectivity
to load the processed data into a PostgreSQL database, ensuring the data is stored efficiently and is ready
for query and analysis.
