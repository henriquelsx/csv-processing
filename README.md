# csv-processing
Developed a scalable, asynchronous processing system for large CSV files with Node.js, RabbitMQ, and microservices. This prevents the API from being blocked by massive uploads while still providing real-time status tracking to the client. This architecture pattern is standard in enterprise systems like ERPs, CRMs, hospital software, and HR systems.

Version 1.0.0

# 📌 What This Project Does

This project provides a complete CSV-processing pipeline composed of three main components:

# 1. API (csv-api)

The API is responsible for:

Receiving CSV file uploads

Storing the file temporarily

Creating a new job record in the PostgreSQL database

Publishing the job to a RabbitMQ queue

In other words:
You upload a CSV → the API registers the job → the worker is notified.

# 2. Worker (csv-worker)

The worker listens to the RabbitMQ queue and processes jobs as they arrive.
For each job, it:

Loads the CSV file

Reads it line by line

Counts:

total_rows

processed_rows

error_rows

Processes each row (your current implementation only iterates — you can add real business logic later)

Calculates processing speed (rows per second)

Updates the job record in PostgreSQL with all metrics

In summary:
The worker handles the heavy lifting and processes the CSV efficiently.

# 3. Database (PostgreSQL — jobs table)

The jobs table stores the full lifecycle of each CSV processing request:

Job ID

Filename & path

Status (pending, processing, finished, failed, etc.)

Timestamps (created_at, started_at, finished_at)

Metrics:

total_rows

processed_rows

error_rows

processing_speed

result_json containing detailed output

This allows you to track every job end-to-end.

🔍 Summary — What You Actually Do With the CSV Data

At the moment, your system performs these tasks:

Reads the CSV line by line

Counts how many rows exist

Tracks processed rows and errors

Measures performance (rows per second)

Saves all results and metrics in the database

There is no row transformation or business rule yet — the worker currently just iterates through the CSV.
You can easily extend it to validate fields, transform data, send to another system, etc.
