# data-engineering-case
We’d like you to build a small but complete data pipeline we can look at and talk through. 
The setup is open on purpose — what we care most about is seeing what you find cool 
in this space and what you’re good at. 
The core task 
Build a pipeline that ingests data from ClinicalTrials.gov (v2 REST API, no auth) into a 
queryable destination, running on a schedule or trigger of your choice. 
Preferred platform: Databricks Free Edition — free, no cloud account, no time limit. Sign 
up at databricks.com/learn/free-edition. Since we work on Databricks, seeing you there is 
useful. If it doesn’t work for you, use an alternative or run it locally (DuckDB, Postgres, 
Parquet on disk — all fine). Just say what you used and why. 
Make it your own 
Once the core is in place, pick one or two things to go deeper on — CI/CD, medallion 
architecture, data quality, metadata/governance, a dashboard or app on top, AI/ML, CDC 
patterns, or something else entirely. One thing done well beats five half-done. 
If you don’t get this far in the time budget, that’s fine — just write down in the README 
what you’d build next and why. 
Practical 
• Time: no more than 6 hours. If you go past, stop and write up the rest. 
• Repo: private is fine — we’d just like read access (we’ll share GitHub usernames). 
Whether you keep it private afterwards is up to you. 
• Deliverable: the repo plus a README covering what you built, your choices and 
why, and what you’d do next. 
• API: https://clinicaltrials.gov/api/v2/studies — JSON, paginated. 
• Language/tools: whatever fits. 
On AI tools 
Use them — we expect it and use them daily. But you own the output: verification, 
validation, and understanding every decision made. We’ll expect you to walk us through and 
defend any part of the code in the follow-up. “The AI did it” won’t fly. 
