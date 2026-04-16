-- Run against: Built-in (Serverless SQL pool)
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'GoldDB')
BEGIN
    CREATE DATABASE GoldDB;
END
