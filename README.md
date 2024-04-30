# Data Warehouse

A simple data warehouse based on PostgreSQL.

## Background

A company specializing in selling various books, aims to separate their transaction storage from the storage intended for analysis.

### Requirements

Stakeholders have outlined several analytical matrices they wish to explore, including:

- Monthly sales trends,
- A list of books and their total sales quantity over time,
- Customer behavior: Average time taken for repeat orders,
- Customer segmentation: Identifying distinct groups of customers based on their purchasing behavior, demographics, or other criteria,
- Profitability analysis: Determining the profitability of different products, customer segments, or sales channels to optimize business strategies,
- Sales forecasting: Predicting future sales trends and demand to optimize inventory levels and resource allocation.

The constructed Data Warehouse should be able to address these analytical questions.

## Project Explanation

### Requirements Gathering & Proposed Solution

The company already has a database containing data about sales transactions.

#### The challenge

1. The company does not have specialized storage for data required for analytical purposes.

#### The solutions

1. __Data warehouse:__ constructing a data warehouse to store data specifically for analytical purposes, separate from daily transactional data.
2. __ELT pipeline:__ establishing a data pipeline to extract data from sources, load the data into the warehouse, and transform the data to make them suitable for analysis.

### Design of Data Warehouse Model

In this project, the data warehouse design implemented the __Kimball Model__, which structures data with the __Dimensional Model__ approach. The implementation of this design model required the following steps:

1. Selecting the business process
2. Declaring the grain
3. Identifying the dimensions
4. Identifying the facts.

#### Selecting the business process

Based on the given requirements, the business process selected for this project is __sales transaction.__

#### Declaring the grain

- A single record represents a book purchase by a customer.

#### Identifying the dimensions

- Customer: `dim_customer`
- Book: `dim_book`
- Date: `dim_date`

#### Identifying the facts

- Sales transaction: `fct_sales_transaction`

#### Data warehouse diagram

### Implementation of Data Pipeline

### Testing Scenario
