# dynasty-agent

A GitHub Actions–based fantasy baseball agent that reads your dynasty league data from Google Sheets, pulls live MLB stats and news, and emails you a daily or weekly digest.

---

## Google Sheets Setup

The agent reads your roster and player data from Google Sheets.  There are two ways to connect:

### Option A — Google Sheets API (recommended, works with private sheets)

Use this option when your sheet is **not** publicly shared, or when you want a more reliable connection that doesn't rely on public CSV exports.

1. Go to [Google Cloud Console](https://console.cloud.google.com/) and open (or create) a project.
2. Enable the **Google Sheets API** and **Google Drive API** for the project.
3. Go to **IAM & Admin → Service Accounts** and create a new service account.
4. Under the service account, go to **Keys → Add Key → Create new key → JSON** and download the file.
5. Open your Google Sheet and share it with the service account's email address (e.g. `my-agent@my-project.iam.gserviceaccount.com`) with at least **Viewer** access.
6. Add the **entire contents** of the downloaded JSON key file as a GitHub secret named `GOOGLE_CREDENTIALS_JSON`.
   - In GitHub: *Settings → Secrets and variables → Actions → New repository secret*
   - Paste the full JSON text (it starts with `{` and ends with `}`) as the secret value.

When `GOOGLE_CREDENTIALS_JSON` is set the agent automatically uses the Sheets API.

### Option B — Public CSV export (no credentials required)

If your Google Sheet is set to **"Anyone with the link can view"**, you can skip the service account setup entirely.  Leave `GOOGLE_CREDENTIALS_JSON` empty and the agent will fall back to the public CSV export URL.

---

## Required GitHub Secrets

| Secret | Description |
|---|---|
| `EMAIL_ADDRESS` | Gmail address used to send reports |
| `EMAIL_PASSWORD` | Gmail app password |
| `RECIPIENT_EMAIL` | Address that receives the reports |
| `GSHEET_ID` | The long ID from your Google Sheet URL |
| `ROSTER_GID` | Tab GID for your roster sheet |
| `AVAILABLE_GID` | Tab GID for the available players sheet |
| `DD_RANK_GID` | Tab GID for Dynasty Dugout rankings |
| `BP_RANK_GID` | Tab GID for Baseball Prospectus rankings |
| `TOP500_GID` | Tab GID for Top 500 dynasty rankings |
| `DRAFTED_GID` | Tab GID listing drafted players (Column E) |
| `GOOGLE_CREDENTIALS_JSON` | *(Optional)* Service account JSON key for private sheets |
| `TWITTER_BEARER_TOKEN` | *(Optional)* Twitter/X API bearer token |

---

## Local Development

```bash
cp .env.example .env
# Fill in .env with your values
pip install -r requirements.txt
RUN_MODE=daily python -u agent.py
```
