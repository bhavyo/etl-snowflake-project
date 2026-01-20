
---resuming subsequent tasks from suspended state---
ALTER TASK PROJECT_DB.TASKS.EMPLOYEE_ANALYTICS_TASK RESUME;

---manually triggering root task---
EXECUTE TASK PROJECT_DB.TASKS.CLEAN_EMPLOYEES_TASK;