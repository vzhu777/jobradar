# JobRadar

JobRadar is an automated job-tracking system that:

- Collects job postings directly from company ATS systems (starting with Workday)
- Stores jobs in Supabase
- Detects new roles automatically
- Sends email alerts only for relevant roles (senior tech / leadership roles in Australia)

This project was built to track technology leadership opportunities across ASX companies and other target organisations.

---

# 🚀 Current Features

### ✅ Workday ATS scraping
- Uses Playwright to load Workday job boards
- Captures internal API responses
- Handles pagination automatically
- Avoids infinite loops and duplicate pages

### ✅ Supabase integration
- Stores companies
- Stores job postings
- Uses upsert logic to avoid duplicates
- Tracks newly created roles

### ✅ Email alerts
- Sends email only when:
  - jobs are newly created
  - AND match relevance filters

### ✅ ASX200 company seeding
- Parses IOZ ETF holdings file
- Seeds ~200 Australian companies automatically

---

# 🏗 Project Structure

