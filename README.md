Library Components
# Features

## CSV Reader
Functionality: This module allows data engineers to read single or multiple CSV files into a designated data processing system.
Features:
- Adaptive scalability catering to file size (1 MB to 10 GB) and quantity (up to 5000 files).
- Auto-detection of header presence in CSV files.
- Support for user-provided headers in cases where files lack headers.
- Capable of handling different file schemas.

## Parquet Transformer
Functionality: Transforms CSV files into Parquet tables, aligning with a developer-supplied file schema.
Features:
 - Field name, data type, and nullability alignment with the developer-supplied file schema.
 - Handling of extra fields in the developer-supplied file schema.
- Consideration for missing fields in the developer-supplied file schema.
- Standardization of date fields to the ISO standard format (yyyy-MM-dd).
Hints:
some columns or some rows in a column are already in the ISO standard format.  Please consider performance!!!
You may want to also consider letting the user of the library choose which column to apply standardization.
