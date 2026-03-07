# Cron Job Scheduling Setup

This document outlines the email scheduling setup, including configuration details for the cron jobs responsible for sending emails at specified times.

## Email Schedule

1. **Spring Training Emails**  
   - **Time:** 2 AM (UTC)  
   - **Frequency:** Every day during the spring training period.
   - **Cron Configuration:**  
     ```bash
     0 2 * * * /path/to/spring_training_email_script.sh
     ```

2. **Daily Emails**  
   - **Time:** 6 AM (UTC)  
   - **Frequency:** Every day.
   - **Cron Configuration:**  
     ```bash
     0 6 * * * /path/to/daily_email_script.sh
     ```

3. **Weekly Emails**  
   - **Time:** 7 AM (UTC) on Mondays  
   - **Frequency:** Once a week.
   - **Cron Configuration:**  
     ```bash
     0 7 * * 1 /path/to/weekly_email_script.sh
     ```

## Environment Variable Setup

Make sure to set the following environment variables for the scripts to function properly:

- `EMAIL_USER`: The email address used to send emails.
- `EMAIL_PASS`: The password for the email account.
- `SMTP_SERVER`: The SMTP server address for sending emails.

### Example of setting environment variables in a shell:
```bash
export EMAIL_USER='your_email@example.com'
export EMAIL_PASS='your_password'
export SMTP_SERVER='smtp.example.com'
```

Ensure these environment variables are accessible by the scripts when they run, possibly by adding them to your `.bashrc` or `.bash_profile`.  

## Conclusion
This setup will automate the email scheduling as per the defined configuration. Please ensure that the scripts referenced in the cron jobs are executable and correctly configured to send emails.