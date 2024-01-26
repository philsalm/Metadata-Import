# Metadata Import
The code in the notebook provides examples of how to import table and column metadata into Databricks Unity Catalog from a data dictionary stored in a separate table, or from text files. Providing detailed metadata within Unity Catalog improves usability for end users, search results, and the quality of the suggestions provided by the Databrick's Assistant.

Please note that the code does execute SQL statements against tables based on the data in the dictionaries. It is assumed that these dictionaries are trusted resources that are not a concern for SQL injection. You can modify the code as needed if this assumption is not true in your environment.
