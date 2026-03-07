# Email Schedule Guide

## Schedule Overview
- **Spring Training**: Emails will be sent at **2 AM CT**.
- **Daily Emails**: Emails will be sent at **6 AM CT**.
- **Weekly Emails**: Emails will be sent every **Monday at 7 AM CT**.

## Setup Instructions
### GitHub Actions
1. Create a `.github/workflows/schedule.yml` file in your repository.
2. Use the following template:
   ```yaml
   name: Email Scheduling

   on:
     schedule:
       - cron: '0 6 * * *' # Daily
       - cron: '0 2 * * *' # Spring Training
       - cron: '0 7 * * MON' # Weekly

   jobs:
     send_email:
       runs-on: ubuntu-latest
       steps:
       - name: Checkout code
         uses: actions/checkout@v2
       - name: Send Email
         run: |
           # Add your email sending logic here
   ```

### Local Cron Jobs
1. Open your crontab configuration:
   ```bash
   crontab -e
   ```
2. Add the following lines:
   ```bash
   0 6 * * * /path/to/email/script.sh    # Daily Email
   0 2 * * * /path/to/spring_training/script.sh  # Spring Training
   0 7 * * 1 /path/to/weekly/script.sh  # Weekly Email
   ```

### Windows Task Scheduler
1. Open Task Scheduler.
2. Create a new task and set the following triggers:
   - Daily at 6 AM
   - Spring Training at 2 AM
   - Weekly on Monday at 7 AM
3. Set the actions to run your email script.

## Required Environment Variables
- `EMAIL_HOST`: Your email server host
- `EMAIL_PORT`: Your email server port
- `EMAIL_USER`: Your email username
- `EMAIL_PASS`: Your email password

## Testing Commands
- You can run your email script manually for testing:
  ```bash
  /path/to/email/script.sh
  ```

## Expected Email Content
- The expected content should include:
  - Subject: `Your Email Subject`
  - Body: `Your email body content`

## Troubleshooting Guide
- If emails are not sent:
  1. Check the email server logs for any errors.
  2. Ensure your environment variables are correctly set.
  3. Verify that your scheduling logic is correctly configured.
  4. Test script execution without the cron/Task Scheduler to debug.

---
This guide is designed to help you set up and manage your email scheduling efficiently.