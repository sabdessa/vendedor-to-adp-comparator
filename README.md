# Project README

## Project Overview

This project is a Java-based command line application that compares data from two different sources (ADP and Vendedor) and identifies differences between them. The application uses PostgreSQL databases and CSV files for data storage and comparison. It is designed to run in different environments (int, stag, prod) and can handle large datasets efficiently by processing data in parallel.
The application is interactive and will require you to type some answers like passwords

## Features

- **Java**: The application is written in Java.
- **Maven**: The project uses Maven for dependency management and build automation.
- **PostgreSQL**: The application connects to PostgreSQL databases to fetch data.
- **CSV Processing**: Data is read from and written to CSV files.
- **Environment Configuration**: Supports different environments (int, stag, prod) with separate configuration files.


## Configuration

The application uses environment-specific configuration files located in the `src/main/resources` directory:

- `config-prod.properties`
- `config-int.properties`

Each configuration file should contain the following properties:

```ini
adp.user=<ADP database user>
vendedor.user=<Vendedor database user>
project=<Google Cloud project ID>
```

## Usage

1. **Build the Project**: Use Maven to build the project.
    ```sh
    mvn clean install
    ```

2. **Run the Application**: Execute the `Main` class.
    ```sh
    java -cp target/your-jar-file.jar com.kramp.Main
    ```

3. **Select Environment**: When prompted, type the environment (int, stag, prod). If no environment is provided, the default is `int`.

4. **Copy Data**: The application will ask if you want to copy data from the databases. Provide the necessary passwords when prompted.

5. **View Results**: The application will generate a `differences.csv` file containing the differences between the ADP and Vendedor data.

## Example

```sh
Type environment (int, stag, prod): 
Default is int
int
Copying data from Data source, Type adp-db password:
<enter adp-db password>
Copying data from Data source, Type vendedor-db password:
<enter vendedor-db password>
Execution time: 12345ms
Similarities: 100
Differences: 50
```