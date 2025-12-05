# Pro Analytics 02 Python Starter Repository

> Use this repo to start a professional Python project.

- Additional information: <https://github.com/denisecase/pro-analytics-02>
- Project organization: [STRUCTURE](./STRUCTURE.md)
- Build professional skills:
  - **Environment Management**: Every project in isolation
  - **Code Quality**: Automated checks for fewer bugs
  - **Documentation**: Use modern project documentation tools
  - **Testing**: Prove your code works
  - **Version Control**: Collaborate professionally

---

## WORKFLOW 1. Set Up Your Machine

Proper setup is critical.
Complete each step in the following guide and verify carefully.

- [SET UP MACHINE](./SET_UP_MACHINE.md)

---

## WORKFLOW 2. Set Up Your Project

After verifying your machine is set up, set up a new Python project by copying this template.
Complete each step in the following guide.

- [SET UP PROJECT](./SET_UP_PROJECT.md)

It includes the critical commands to set up your local environment (and activate it):

```shell
uv venv
uv python pin 3.12
uv sync --extra dev --extra docs --upgrade
uv run pre-commit install
uv run python --version
```

**Windows (PowerShell):**

```shell
.\.venv\Scripts\activate
```

**macOS / Linux / WSL:**

```shell
source .venv/bin/activate
```

---

## WORKFLOW 3. Daily Workflow

Please ensure that the prior steps have been verified before continuing.
When working on a project, we open just that project in VS Code.

### 3.1 Git Pull from GitHub

Always start with `git pull` to check for any changes made to the GitHub repo.

```shell
git pull
```

### 3.2 Run Checks as You Work

This mirrors real work where we typically:

1. Update dependencies (for security and compatibility).
2. Clean unused cached packages to free space.
3. Use `git add .` to stage all changes.
4. Run ruff and fix minor issues.
5. Update pre-commit periodically.
6. Run pre-commit quality checks on all code files (**twice if needed**, the first pass may fix things).
7. Run tests.

In VS Code, open your repository, then open a terminal (Terminal / New Terminal) and run the following commands one at a time to check the code.

```shell
uv sync --extra dev --extra docs --upgrade
uv cache clean
git add .
uvx ruff check --fix
uvx pre-commit autoupdate
uv run pre-commit run --all-files
git add .
uv run pytest
```

NOTE: The second `git add .` ensures any automatic fixes made by Ruff or pre-commit are included before testing or committing.

<details>
<summary>Click to see a note on best practices</summary>

`uvx` runs the latest version of a tool in an isolated cache, outside the virtual environment.
This keeps the project light and simple, but behavior can change when the tool updates.
For fully reproducible results, or when you need to use the local `.venv`, use `uv run` instead.

</details>

### 3.3 Build Project Documentation

Make sure you have current doc dependencies, then build your docs, fix any errors, and serve them locally to test.

```shell
uv run mkdocs build --strict
uv run mkdocs serve
```

- After running the serve command, the local URL of the docs will be provided. To open the site, press **CTRL and click** the provided link (at the same time) to view the documentation. On a Mac, use **CMD and click**.
- Press **CTRL c** (at the same time) to stop the hosting process.

### 3.4 Execute

This project includes demo code.
Run the demo Python modules to confirm everything is working.

In VS Code terminal, run:

```shell
uv run python -m analytics_project.demo_module_basics
uv run python -m analytics_project.demo_module_languages
uv run python -m analytics_project.demo_module_stats
uv run python -m analytics_project.demo_module_viz
```

You should see:

- Log messages in the terminal
- Greetings in several languages
- Simple statistics
- A chart window open (close the chart window to continue).

If this works, your project is ready! If not, check:

- Are you in the right folder? (All terminal commands are to be run from the root project folder.)
- Did you run the full `uv sync --extra dev --extra docs --upgrade` command?
- Are there any error messages? (ask for help with the exact error)

---

### 3.5 Git add-commit-push to GitHub

Anytime we make working changes to code is a good time to git add-commit-push to GitHub.

1. Stage your changes with git add.
2. Commit your changes with a useful message in quotes.
3. Push your work to GitHub.

```shell
git add .
git commit -m "describe your change in quotes"
git push -u origin main
```

This will trigger the GitHub Actions workflow and publish your documentation via GitHub Pages.

### 3.6 Modify and Debug

With a working version safe in GitHub, start making changes to the code.

Before starting a new session, remember to do a `git pull` and keep your tools updated.

Each time forward progress is made, remember to git add-commit-push.


### SmartSales Setup
- Created new file 'src/analytics_project/data_prep.py' for C:/Repos
  - Cloned repo to local drive.
    - Repo cloned [smart-sales-starter-files](https://github.com/denisecase/smart-sales-starter-files) GitHub repo
    - Installed recommended extensions to VS Code

- Set up Virtual Enviroment
  - Opened VS Code, open new terminal in powershell
  - Ran the following lines
    - Create virtual enviroment : uv venv
    - Pin Python version (3.12) : uv python pin 3.12
    - Install all dependencies  : uv sync -- extra dev --extra docs -- upgrade
    - Enable pre-commit checks, automatically runs each commit : uv run pre-commit install
    - Verify Python version : uv run python --version

### Run Data Prep Module and Commit
- Opened the terminal
-  Ran module using
-  uv run python -m analytics_project.data_prep
   -  git add, commit, push
      -  git add .
      -  git commit -m "add starter files"
      -  git push -u origin main

# P3: Prep Data for ETL

# P4: Create and Populate DW
### Introductions of new folders/files added
- data/warehouse/smart_sales.db
  - Location of dw, made through sqlite3
- etl_to_dw
- data_scrubber.py
  - Module including general instructions/operations for cleaning raw data

### Run etl_to_dw
- Open terminal
- Run module using
  - uv run python -m analytics_project.dw.etl_to_dw

### etl_to_dw
- Python module performs a series of operations for the ETL process. (Extract, Transform, Load)
  - Extraction: Reads CSV files from data/prepared/
  - Schema Creation: Creates tables corresponding to extracted files. Customers, Products, Sales.
  - Transform
    - Rename columns to match schema theme.
    - Select relevant columns for each table.
    - Ensure data types are compatible.
  - Load: Load to data/warehouse/smart_sales.db

### TO DO: Added Images, table dimensions, etc.

### Challenges encountered
- TO BE ADDED

# P5: Cross-Platform Reporting with Power BI
### Connecting data-warehouse to BI Tools, Power BI.
- Requirements:
  - Power BI Desktop
  - SQLite ODBC Driver
  - Create DSN named SmartSalesDSN
- Process
  - Load tables to Power BI
    - Power BI -> Data (Tab) -> Get Data -> Other
    - Select new DSN: SmartSalesDSN
- TO DO: Add what was done
  - SQL Query
  - Slicing
  - Dicing
  - Drilldown
  - Images
# P6: BI Insight, Storytelling, and Engagement
  - OLAP Creation
  - Create OLAP calculating for new metrics for further analysis and insights
  - OLAP output to new csv data/olap_cubing_outputs/multidimensional_olap_cube.csv
# P7 Custom BI Project
  # Section 1. Business Goal
  - Identify regional product sales trends to identify product and store sales performance
  # Section 2.
  - Dataset from warehouse
  - Columns
    - units_sold, total_cogs, total_sales_revenue, average_gross_profit, average_selling_price
    - product_category, product_item
    - region, sale_quarter
  - Time Period: 2025
  # Section 3: Tools Used
  - Primary tools:
    - VS Code - Data cleaning, ETL, and OLAP processes
    - Power BI - Data transformation and visualiazation
  - Data Source:
    - CSV file imports
    - Data warehouse - smart_sales.db
  - Features Utilized:
    - Interactive slicers for filtering
    - Cross-filtering between visuals
    - Conditional Formatting
    - Dashboard
  # Section 4: Workflow & Logic
  - Data Import Process
    - Clean file data from data/raw
    - Build OLAP utulizaing data/prepared
      - Utulize OLAP to generate new metric and calculations
      - Export to new CSV
    - Import CSV to Power BI
  - Data Transformation
    - New columns generated
      - units_sold
      - total_cogs
      - total_sales_revenue
      - average_gross_profit
      - average_selling_price
      - sale_growth_pct
      - sale_quarter - Parsed from sales_date
    - Analytical Techniques
    - Slicing & Dicing: Region, Sales Quarter
    - Drilldown: Region, Qtr ,Product item
    - Aggregation: AVG, SUM, COUNT,
    - Trend Analysis: Time series visualization
    -
