# Distributed Job Scheduler

A simple distributed job scheduling system built using **Go, PostgreSQL, HTML, CSS, and JavaScript**.

## 📌 Problem Statement

Managing and executing background jobs manually can be difficult when there are many jobs to process.
This project provides a system where users can create, schedule, manage, and monitor jobs from a web interface.

## 💡 Solution

The Distributed Job Scheduler allows users to:

* Create jobs
* Schedule jobs for execution
* Store job information in PostgreSQL
* Process jobs using Go
* Monitor job status
* View job details through a web dashboard

## 🚀 Features

* Create and schedule jobs
* Job queue management
* Job status tracking
* Background job processing
* Retry failed jobs
* PostgreSQL database storage
* REST API using Go
* Simple web dashboard
* Distributed job processing

## 🛠️ Technologies Used

### Backend

* Go (Golang)
* REST API
* PostgreSQL

### Frontend

* HTML
* CSS
* JavaScript

### Tools

* Git
* GitHub

## ⚙️ How to Run

### 1. Clone the repository

```bash
git clone https://github.com/Gouri-Pawar/distributed-job-scheduler.git
cd distributed-job-scheduler
```

### 2. Install dependencies

```bash
go mod download
```

### 3. Configure PostgreSQL

Create a PostgreSQL database and configure the required database connection details in the project.

### 4. Run the application

```bash
go run ./cmd/api
```

### 5. Open the web interface

Open the frontend in your browser using the URL provided by the application.

## 🔄 How It Works

```text
User
  ↓
Web Dashboard
  ↓
Go REST API
  ↓
Job Scheduler
  ↓
Job Queue
  ↓
Worker
  ↓
PostgreSQL
```

1. The user creates a job from the web dashboard.
2. The frontend sends the job to the Go backend.
3. The scheduler stores and manages the job.
4. Workers pick up available jobs.
5. The job is executed.
6. The job status is updated in PostgreSQL.
7. The user can monitor the result from the dashboard.

## 📊 Job Status

A job can have different statuses:

* `Pending` — Job is waiting to be processed.
* `Running` — Job is currently being executed.
* `Completed` — Job finished successfully.
* `Failed` — Job execution failed.
* `Retrying` — Job will be executed again.

## 🔮 Future Improvements

* User authentication
* Multiple worker nodes
* Real-time job updates
* Advanced scheduling using cron expressions
* Job priority
* Better monitoring and logging
* Docker support
* Cloud deployment

