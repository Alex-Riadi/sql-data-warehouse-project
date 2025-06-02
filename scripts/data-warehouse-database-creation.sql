/*
===========================================================
Create Database and Schemas
===========================================================
Script Purpose:
	Creating Database, with three different schemas
*/

-- Creating Database 'DataWarehouse'
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
