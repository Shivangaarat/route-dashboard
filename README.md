# Route Pattern Intelligence Dashboard

Next.js 15 app on Vercel — analyses daily dispatch data, detects route patterns,
scores drivers, and auto-ingests Excel reports from a dedicated email inbox.

---

## Stack

| Layer | Technology |
|---|---|
| Frontend + API | Next.js 15 (App Router) |
| Hosting | Vercel (free tier) |
| Database | Neon Postgres (free tier) |
| Scheduling | Vercel Cron Jobs |
| Mailbox | IMAP via imapflow |
| Excel I/O | xlsx (parse) + exceljs (export) |

---

## Deployment — exact steps

### 1. Push to GitHub
```bash
# In the unzipped folder (where package.json lives):
git init
git add .
git commit -m "initial"
gh repo create route-dashboard --public --push
```

### 2. Create Neon database (free)
1. Go to https://neon.tech → Sign up → New Project
2. Name it anything (e.g. `route-dashboard`)
3. Copy the **Connection string** — looks like:
   `postgresql://user:pass@ep-xxx.us-east-2.aws.neon.tech/neondb?sslmode=require`

### 3. Deploy to Vercel
1. Go to https://vercel.com → New Project → Import your GitHub repo
2. Framework: Next.js (auto-detected)
3. **Before clicking Deploy**, go to Environment Variables and add:

| Variable | Value |
|---|---|
| `DATABASE_URL` | Your Neon connection string from step 2 |
| `MAIL_HOST` | `imap.gmail.com` (or `outlook.office365.com`) |
| `MAIL_PORT` | `993` |
| `MAIL_USER` | `your-dispatch-mailbox@gmail.com` |
| `MAIL_PASSWORD` | Your Gmail App Password (see below) |
| `MAIL_SUBJECT_FILTER` | `Dispatch Report` |
| `CRON_SECRET` | Any random string, e.g. `abc123xyz789` |

4. Click **Deploy**

### 4. Run database migration (once)
After first deploy succeeds, open Vercel → your project → Functions tab,
or run locally:
```bash
npm install
cp .env.local.example .env.local
# Paste your DATABASE_URL into .env.local
node lib/migrate.js
```

### 5. Done
Your dashboard is live at `https://your-project.vercel.app`

---

## Gmail App Password setup
1. Google Account → Security → enable 2-Factor Authentication
2. Search "App Passwords" → create one for "Mail"
3. Copy the 16-character password → use as `MAIL_PASSWORD`

For Outlook/Office365:
```
MAIL_HOST = outlook.office365.com
MAIL_PORT = 993
```

---

## How auto-scheduling works

`vercel.json` registers a cron:
```json
{ "path": "/api/cron/check-mailbox", "schedule": "0 6 * * 1-5" }
```
Every weekday at **6:00 AM UTC**, Vercel calls that endpoint.
It connects to your IMAP mailbox, finds unread emails matching
`MAIL_SUBJECT_FILTER`, downloads the Excel attachment, parses it,
scores drivers, detects patterns, saves everything to Neon, then
marks the email as read.

**To send data:** Email the Excel file to your dispatch mailbox
with subject containing "Dispatch Report". Done.

---

## Excel column names accepted

| Field | Accepted headers |
|---|---|
| Route name | `route_name`, `Route Name`, `RouteName` |
| Date | `date`, `Date`, `DATE` |
| Zone | `zone`, `Zone` |
| Team | `team`, `Team` |
| Driver ID | `driver_id`, `Driver ID`, `DriverID` |
| Sites | `sites`, `Sites` (pipe-separated: `S001\|S003\|S007`) |
| Volume | `volume`, `Volume` |
| Drops | `drops`, `Drops` |
| Successful | `successful`, `Successful` (optional — auto-calc if missing) |
| Rejections | `rejections`, `Rejections` |
| Helpers | `helpers`, `Helpers` |
| Rejection reason | `rej_reason`, `Rejection Reason` |

---

## Project structure

```
/                          ← repo root (this is where package.json must be)
├── app/
│   ├── api/
│   │   ├── cron/check-mailbox/route.js   ← runs 6am Mon–Fri
│   │   ├── ingest/route.js               ← manual upload
│   │   ├── history/route.js              ← date list + snapshots
│   │   └── export/route.js              ← Excel/CSV download
│   ├── dashboard/page.jsx               ← React dashboard UI
│   ├── layout.jsx
│   └── page.jsx                         ← redirects to /dashboard
├── lib/
│   ├── engine.js                        ← pattern detection + scoring
│   ├── mailbox.js                       ← IMAP reader
│   ├── db.js                            ← Neon database operations
│   └── migrate.js                       ← run once to create tables
├── vercel.json                          ← cron config
├── next.config.js
├── package.json                         ← must be at root
└── .env.local.example
```

> **Important:** When you unzip, make sure `package.json` is at the **root**
> of your git repo — not inside a subfolder. Vercel looks for it at root.
> If you see `package.json` inside a `route-dashboard/` folder, move everything up one level.

