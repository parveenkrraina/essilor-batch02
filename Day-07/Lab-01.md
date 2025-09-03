# Hands-On Lab 1: Workflow and Jobs Compute Basics

## Lab Objective
Gain hands-on experience by creating, configuring, and running a Databricks job using a notebook and Jobs Compute.

---

## Instructions

### 1. Create Your Own Notebook
- In your workspace, create a new **Python notebook**.

Paste the following code into your new notebook:

```python
# Databricks notebook source
dbutils.widgets.text("input", "world", "Input Message")
input_val = dbutils.widgets.get("input")
print(f"Hello, {input_val}!")

import pandas as pd
data = pd.DataFrame({'val': range(5)})
display(data)
```

- Save the notebook with a meaningful name (e.g., `MyFirstNotebook`).

### 2. Create a Job
- Go to **Jobs & Pipeline > Jobs**.
- Click **Create Job**.
- Name your job: `MyFirstJob`.
- Set the job type to **Notebook**.
- Set the Path to your notebook (e.g., `/Users/<your_username>/MyFirstNotebook`).
- Use **Jobs Compute** as the cluster type created in earlier labs.

### 3. Add a Parameter
- Set parameter input to your name (e.g., `input = "Parveen KR."`).

### 4. Run the Job
- Click **Run Now**.
- After the run, review:
    - Output logs (should print `"Hello, Parveen KR.!"` or your name).
    - Displayed DataFrame.
    - Cluster type used.

---

## Checkpoint Questions

1. **What cluster type did you use? Why is it preferred for scheduled jobs?**

2. **What output do you see from the notebook?**

3. **Where do you find the job run logs?**