<!--  Purpose: Tell Claude (or any LLM) EXACTLY how to interact with this repo.
      Scope : Medallion lakehouse on PostgreSQL, authored with SQLModel + PySpark + Airflow.
      Audience: Data engineers & ML engineers working in VS Code (no notebooks). -->

---

🔑  1. Project Mission
> Build a **local-first, cloud-portable** data platform that uses the Medallion pattern
> (bronze → silver → gold) and can be lifted into Databricks (Delta Live Tables) or
> Snowflake (Snowpark) with minimal friction.

* **Storage & compute**: PostgreSQL 15 for both raw landing and curated marts.  
* **Schemas-as-code**: [`SQLModel`](https://sqlmodel.tiangolo.com/) (SQLAlchemy + Pydantic).  
* **Transformations**: PySpark ⇢ stay Databricks/Snowpark compatible.  
* **Orchestration**: Apache Airflow DAGs (Python-native).  
* **Dev workflow**: VS Code + PyEnv (.venv) + Poetry + pre-commit + pytest; **no notebooks**.  

---

🔧  2. Repository Layout (summary)
.
├── .venv/ # PyEnv based Python virtual environment, built using Poetry + pyproject.toml
│   └── ...
├── airflow/ # DAG definitions
│   └── dags/<layer>/<task>.py
├── src/
│   ├── _base/ # contains classes which can be inherited or used anywhere
│   │   ├── columns/ # SQLModel common column classes
│   │   │   ├── ts_tuple_range.py
│   │   │   └── …
│   │   ├── functions/ # SQLAlchemy common function definitions (DDL)
│   │   │   ├── calculate_systime_range.py
│   │   │   └── …
│   │   └── tables/ # SQLModel base table classes
│   │       ├── history_table.py
│   │       └── …
│   ├── <<database_1>>/ # a folder named to correspond to a database or data lake (collection of schemas, tables, views, etc)
│   │   ├── <<database_1>>.py  # function used to build database if not exists
│   │   ├── data/ # SQLModel reference data classes
│   │   │   ├── common.py
│   │   │   ├── <<schema_1>>.py
│   │   │   └── …
│   │   ├── schemas/ # SQLModel schema classes
│   │   │   ├── common.py
│   │   │   ├── <<schema_1>>.py
│   │   │   └── …
│   │   └── tables/ # SQLModel base table classes
│   │       ├── common/
│   │       │   └── table_1.py
│   │       ├── <<schema_1>>/
│   │       │   └── table_2.py
│   │       └── …
│   │   └── transforms/ # PySpark jobs designed to populate or transform entities within this db
│   │       └── …
│   ├── utils/ # Connection helpers, logging, etc.
│   └── config.py # Single source of env truth
├── docker-compose.yml # local PG + Airflow + Spark
├── tests/
├── scripts/ # one shot admin scripts
└── Claude.md # ★ you are here

**Convention:** File/folder names == snake_case; SQLModel class names == `CamelCase`.

---

🧑‍💻  3. How to ask Claude for help (critical!)

1. **Always specify the layer** (`bronze`, `silver`, `gold`) *and* the artefact type  
   (`model`, `transform`, `dag`, `test`). Claude is taught to look in those folders.  
2. **Include acceptance criteria** (row count deltas, hash totals, KPI outputs).  
3. **Stateless prompts** - every prompt must re-state context; Claude does not inspect
   your local git diff.  
4. **Prefer declarative instructions** ("Produce a SQLModel for *fact_payment* with
   temporal system-time columns") over step-by-step chat.  
5. **One file at a time**: Ask Claude to *insert* or *append* code blocks exactly as they
   should appear-no narrative inside the file.

Example prompt ⭢ see section 8.

---

🏗️  4. Creating & Updating Schemas

### 4-A Regular tables  
* Derive from `SQLModel`, set `table=True`, add `__tablename__ = "br_<name>"` etc.  
* Use *snake_case* field names; let SQLModel generate type hints.  
* Keep DDL-only metadata (indexes, partitioning) in the class-level `__table_args__`.

### 4-B  Temporal tables (silver layer)
```python
class DimCustomerHist(SQLModel, table=True):
    __tablename__ = "dim_customer_hist"
    __table_args__ = (
        {"postgresql_with": "system_versioning = true"},  # PG ≥17 extension
    )

    customer_id: int = Field(primary_key=True)
    first_name: str
    …
    sys_start: datetime = Field(sa_column=Column(TIMESTAMP(timezone=True),
                                                 primary_key=True,
                                                 default=text("clock_timestamp()")))
    sys_end: datetime = Field(sa_column=Column(TIMESTAMP(timezone=True),
                                               default=text("'infinity'")))
Claude should:

Infer __tablename__ from class name if not supplied.

Add sys_start/sys_end automatically on *_hist tables.

Generate a paired current-state view in models/views/ when asked.

---

🔄  5. Transformations with PySpark
Each job lives under src/transforms/<layer>/<name>.py.

follows signature def run(spark: SparkSession, cfg: Config): …

Input tables loaded via spark.read.format("jdbc") or Delta/Snowpark when ported.

Output written back with .mode("append") for bronze, .mode("overwrite") for
deterministic silver/gold snapshots.

Keep business logic in pure Spark SQL - easy to copy into Databricks notebooks later.

---

🪁  6. Airflow DAG Conventions
One DAG per layer, parameterised by execution_date.

Use the helper factory in src/utils/dag_factory.py:

dag = dag_factory(
    dag_id="silver_load",
    schedule="@daily",
    default_args=DEFAULT_ARGS,
    tasks=[
        ETLTask("rental_to_silver", transform="rental_silver.py"),
        QualityTask("dq_rental_nulls"),
    ],
)
Task naming: <source>_to_<layer> for loads, dq_<entity>_<rule> for data-quality.

---

⚡ 7. Python Virtual Environment & Code Execution

**CRITICAL**: This project uses Poetry + PyEnv with a pre-built `.venv/` directory containing all dependencies.

### 🔧 Determining the Correct Python Path

**ALWAYS** check the `.venv` directory structure first to determine the correct Python executable path:

```bash
# Check .venv directory structure
ls -la .venv/

# Look for either Scripts/ (Windows) or bin/ (Linux/Mac)
ls -la .venv/Scripts/  # Windows virtualenv
ls -la .venv/bin/      # Linux/Mac virtualenv
```

### 🚀 Running Python Code:

#### **Windows Virtual Environment (most common in this project):**
- **✅ CORRECT**: `.venv/Scripts/python.exe script_name.py`
- **✅ CORRECT**: `.venv/Scripts/python.exe` (for interactive)
- Available tools: `.venv/Scripts/pytest.exe`, `.venv/Scripts/ruff.exe`, etc.

#### **Linux/Mac Virtual Environment:**
- **✅ CORRECT**: `.venv/bin/python script_name.py`
- **✅ CORRECT**: `.venv/bin/python` (for interactive)
- Available tools: `.venv/bin/pytest`, `.venv/bin/ruff`, etc.

#### **❌ WRONG (Never Use):**
- `python script_name.py` (system Python, missing dependencies)
- `python3 script_name.py` (system Python3, missing dependencies)

### 🐧 WSL (Windows Subsystem for Linux) Special Case:

When working in WSL with a Windows-created `.venv`:
- The `.venv/Scripts/` directory will exist (Windows structure)
- Windows `.exe` files can be executed directly from WSL
- **Use**: `.venv/Scripts/python.exe script_name.py`

### 🛠️ Key Tools Available in .venv:
- `python.exe`/`python` - Python 3.12 with all project dependencies
- `pytest.exe`/`pytest` - Unit testing framework
- `ruff.exe`/`ruff` - Fast Python linter and formatter  
- `black.exe`/`black` - Code formatter
- `pre-commit.exe`/`pre-commit` - Git hooks for code quality

### 📦 Dependencies Already Installed:
All dependencies from `pyproject.toml` are pre-installed in `.venv/`, including:
- SQLModel, SQLAlchemy, psycopg2-binary (database)
- FastAPI, Pydantic (API framework)
- pytest (testing)
- ruff, black, pre-commit (code quality)

### 🔍 Troubleshooting Virtual Environment Issues:

1. **"Module not found" errors**: Always indicates wrong Python path
2. **"No such file or directory"**: Check if using Windows vs Linux path structure
3. **Encoding issues with emojis**: Fixed in `utils/utils.py` configure_logging function

**When helping with this project**: 
1. Always check `.venv/` structure first
2. Use appropriate path based on directory structure found
3. Never assume system Python has the required dependencies

---

🏗️  8. Database Orchestration & Table Type Classification

### Master Build System  
Use `scripts/orchestrate_build.py` for coordinated database management:

```bash
# Rebuild everything from scratch
.venv/Scripts/python.exe scripts/orchestrate_build.py

# Rebuild specific database  
.venv/Scripts/python.exe scripts/orchestrate_build.py postgres demo

# With debug logging
.venv/Scripts/python.exe scripts/orchestrate_build.py postgres demo DEBUG
```

### Individual Build Scripts (Testable Units)
Each script is parameterized and independently testable:

```bash
# Create databases only
.venv/Scripts/python.exe scripts/build_databases.py postgres demo INFO postgres localhost 5432

# Create schemas only
.venv/Scripts/python.exe scripts/build_schemas.py postgres demo INFO postgres localhost 5432

# Create tables only  
.venv/Scripts/python.exe scripts/build_tables.py postgres demo INFO postgres localhost 5432

# Populate reference data only
.venv/Scripts/python.exe scripts/build_reference_data_improved.py postgres demo INFO postgres localhost 5432
```

**Design Philosophy**: Composition over duplication. The orchestrator coordinates existing scripts rather than duplicating their logic.

### Build Script Architecture Philosophy

Each `build_xxx.py` script follows a **dual interface pattern** for maximum flexibility:

#### **Library Functions** (Importable Core Logic)
```python
def create_tables_for_database(engine: Engine, src_path: str, database_name: str) -> None:
def create_tables_for_all_databases(db_engine_type: str, user_name: str, host: str, port: int, src_path: str) -> None:
```

#### **CLI Interface** (Complete Standalone Units)
```python
if __name__ == "__main__":
    try:
        # Parameter parsing with defaults
        # Logging configuration  
        # Engine creation
        # Error handling with proper exit codes
        sys.exit(0)  # Success
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)  # Failure
```

### **Critical Design Principles**

1. **DRY (Don't Repeat Yourself)**: One way to do each operation
   - ✅ Use CLI composition for orchestration
   - ❌ Don't import library functions and duplicate defensive coding

2. **Functional Decomposition**: Small, testable units
   - Each script has single responsibility
   - Each script can be tested in isolation
   - Each script has proper error handling with exit codes

3. **Composable Architecture**: Scripts designed for orchestration
   - Consistent parameter patterns: `engine_type db_name log_level user host port`
   - Proper exit codes: `0 = success`, `1 = failure`
   - Both single-item and batch modes supported

4. **Error Handling Strategy**:
   ```python
   # Enhanced scripts now properly signal failures
   try:
       # Core operations
       logger.info("Process completed successfully")
       sys.exit(0)
   except Exception as e:
       logger.error(f"Fatal error: {e}")
       sys.exit(1)
   ```

### **Testing Individual Components**
```bash
# Test each component independently
.venv/Scripts/python.exe scripts/build_databases.py postgres demo INFO postgres localhost 5432
echo $?  # Should be 0 for success, 1 for failure

.venv/Scripts/python.exe scripts/build_schemas.py postgres demo INFO postgres localhost 5432
echo $?  # Should be 0 for success, 1 for failure
```

### **When to Use Which Approach**
- **Orchestration**: Always use `orchestrate_build.py` (CLI composition)
- **Testing**: Test individual `build_xxx.py` scripts directly
- **Advanced scripting**: Import library functions only when you need custom logic
- **Debugging**: Run individual scripts to isolate issues

### Table Type Classification
Tables are automatically classified into three types:

#### Reference Tables (Lookup Data)
- **Inherits from**: `ReferenceTableBase` or `ReferenceTableMixin`
- **Primary Keys**: SMALLINT for performance
- **Required Fields**: `is_active: bool`, `systime: datetime`
- **Naming**: `*_type`, `*_status_type`
- **Example**: `TradeStatusType`, `ActionType`

#### Transactional Tables (Business Entities)  
- **Inherits from**: `TransactionalTableBase` or `TransactionalTableMixin`
- **Primary Keys**: UUID for business meaning
- **Required Fields**: `effective_time: datetime`
- **Example**: `Trade`, `Portfolio`, `Instrument`

#### Temporal Tables (History Tracking)
- **Inherits from**: `TemporalTableBase` or `TemporalTableMixin`
- **Required Fields**: `sys_start: datetime`, `sys_end: datetime`
- **Naming**: `*_hist`
- **Auto-triggers**: Automatically configured for history tracking

### What the Orchestrator Does:
1. **🔍 Discovery**: Auto-finds all database projects in `/src/`
2. **🏗️  Creation**: Creates databases, schemas, tables in dependency order
3. **⚡ Triggers**: Auto-configures temporal triggers based on table type
4. **📊 Reference Data**: Populates lookup tables with error recovery
5. **🔍 Validation**: Checks schema conventions and reports issues
6. **📋 Reporting**: Comprehensive success/failure tracking

### Table Type Mixins Usage:
```python
# Reference table example
from src._base.tables.table_type_mixins import ReferenceTableBase

class TradeStatusType(ReferenceTableBase):
    __tablename__ = "trade_status_type"
    __table_args__ = {"schema": "demo"}
    
    trade_status_type_id: int = Field(primary_key=True, sa_type=SMALLINT)
    trade_status_type_name: str = Field(nullable=False, max_length=50)
    # is_active and systime inherited from ReferenceTableBase

# Transactional table example  
from src._base.tables.table_type_mixins import TransactionalTableMixin

class Trade(TransactionalTableMixin, SQLModel, table=True):
    __tablename__ = "trade"
    __table_args__ = {"schema": "demo"}
    
    trade_id: UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    # effective_time inherited from TransactionalTableMixin
```

---

✅  9. Testing & CI
Unit tests live in tests/ and use an ephemeral Postgres via pytest-docker.

Data tests (row counts, hash totals) are in Airflow QualityTasks and fail the DAG.

Pre-commit hooks enforce ruff, black, sqlfluff.