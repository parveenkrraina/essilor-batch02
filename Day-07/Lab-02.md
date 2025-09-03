# Lab 02: Automating a Databricks Job with Scheduler

## Lab Objective
Practice automating a Databricks job using the built-in scheduler.

---

## Instructions

1. **Edit Your Job Schedule**
    - Open your Databricks job.
    - Add a schedule to run every 2 hours (or another interval as desired).
    - Example cron expression for every 2 hours: `0 */2 * * *`
    - For testing, you can temporarily set the schedule to every 5 minutes: `*/5 * * * *`

2. **Save the Schedule**
    - Click **Save** after editing the schedule.

3. **Simulate Multiple Runs**
    - Wait for or simulate multiple runs using the 5-minute interval.
    - Observe the job's run history.

4. **Observe Run History**
    - Go to the **Runs** tab to view execution history and status.

5. **Configure Email Notifications**
    - Set up email notifications for job success or failure in the job settings.

---

## Checkpoint Questions

1. **What cron expression did you use for scheduling?**
    - Example: `0 */2 * * *` (every 2 hours)

2. **How does scheduling help automate data pipelines?**
    - Scheduling enables automated, consistent, and timely execution of data pipelines without manual intervention.

3. **Where can you find and interpret run history?**
    - Run history is available in the **Runs** tab of the job. It shows execution times, statuses, and logs for each run.

---

## Optional Bonus: Experiment with Parameters

- Edit your job to change parameter values.
- Observe how the notebook output changes for each run with different parameters.

---

## Submission/Review

- Take a screenshot of your job’s run output and schedule screen.
- Submit your answers to the checkpoint questions above.
