# Business Rules — Employee Pipeline

## Salary Handling
- Invalid or non-numeric salaries are converted to NULL
- Employees with NULL salary are excluded from salary-based analytics

## High Earner Logic
- An employee is considered a high earner if salary >= 80,000

## Ranking Logic
- Salary ranking is department-specific
- Dense ranking is used
- Only employees with valid salary are ranked

## Date Handling
- Invalid dates are coerced to NULL
- Tenure is calculated using the current date

## Text Normalization
- Department names are standardized using title case
- Employee names are preserved as received