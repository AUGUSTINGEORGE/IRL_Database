from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker
import random
import uuid
import pandas as pd
import sys
import logging
import os
from sqlalchemy.exc import IntegrityError

print("Python executable being used:", sys.executable)
print("Python version:", sys.version)


Base = declarative_base()

# Define tables based on ERD schema
class Project(Base):
    __tablename__ = 'projects'
    project_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

class Task(Base):
    __tablename__ = 'tasks'
    task_id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey('projects.project_id'))
    task_code = Column(String, unique=True, nullable=False)
    description = Column(String)
    partner = Column(String)
    contact_person = Column(String)
    email = Column(String)

class Method(Base):
    __tablename__ = 'methods'
    method_id = Column(Integer, primary_key=True)
    task_id = Column(Integer, ForeignKey('tasks.task_id'))
    method_type = Column(String)
    name = Column(String)
    objective = Column(String)
    maturity = Column(String)
    category = Column(String)
    unique_id = Column(String, unique=True)

class Technology(Base):
    __tablename__ = 'technologies'
    technology_id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    description = Column(String)

class MethodTechnologyService(Base):
    __tablename__ = 'method_technology_services'
    service_id = Column(Integer, primary_key=True)
    method_id = Column(Integer, ForeignKey('methods.method_id'))
    technology_id = Column(Integer, ForeignKey('technologies.technology_id'))
    maturity_min = Column(Float)
    maturity_max = Column(Float)
    cost_min = Column(Float)
    cost_max = Column(Float)
    interoperability_min = Column(Float)
    interoperability_max = Column(Float)
    integration_min = Column(Float)
    integration_max = Column(Float)

# Create the SQLite database
engine = create_engine('sqlite:///fuel_cell_database.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

def extract_methods_from_excel(file_path):
    """
    Extract methods data from the Excel file safely using context management.
    Skips headers and non-data rows.
    """
    try:
        with pd.ExcelFile(file_path) as xls:  # Use context manager to release the file immediately
            methods_data = []
            for sheet in xls.sheet_names:
                if sheet.startswith("Methods T"):  # Process only relevant sheets
                    df = pd.read_excel(xls, sheet_name=sheet)

                    # Ensure the required columns exist in the sheet
                    required_columns = [
                        "Associated to DECODE Task",
                        "Name of the method or technique",
                        "Method type",
                        "Method maturity"
                    ]
                    if not all(col in df.columns for col in required_columns):
                        logging.warning(f"Sheet {sheet} does not have the required columns. Skipping.")
                        continue

                    # Process rows, skipping non-data rows (e.g., headings, NaNs)
                    for _, row in df.iterrows():
                        task_code = row.get("Associated to DECODE Task")
                        method_name = row.get("Name of the method or technique")
                        method_type = row.get("Method type")
                        maturity = row.get("Method maturity")

                        # Skip rows with missing critical data
                        if pd.isnull(task_code) or pd.isnull(method_name):
                            logging.info(f"Skipping incomplete row: {row}")
                            continue

                        # Clean and validate data
                        method_type = method_type.strip() if pd.notnull(method_type) else "Uncategorized"
                        maturity = maturity.strip() if pd.notnull(maturity) else "TRL 5"

                        methods_data.append({
                            "task_code": task_code.strip(),
                            "name": method_name.strip(),
                            "method_type": method_type,
                            "maturity": maturity
                        })
                        logging.info(f"Extracted Method: {method_name}, Type: {method_type}, Maturity: {maturity}")

            return methods_data
    except Exception as e:
        logging.error(f"Error reading Excel file: {e}")
        return []

def populate_data():
    """
    Populate the database with data extracted from the Excel file.
    """
    try:
        logging.info("Starting database population...")

        # Begin a new transaction
        session.begin()

        # Step 1: Define the Excel file path
        excel_file_path = os.getenv("EXCEL_FILE_PATH", "C:/Users/saint/Downloads/IRL_setup-main/IRL_setup-main/decodefactsheet_copy.xlsx")
        if not os.path.exists(excel_file_path):
            raise FileNotFoundError(f"Excel file not found at {excel_file_path}")

        # Step 2: Extract methods from the Excel file
        logging.info("Extracting data from the Excel file...")
        methods_data = extract_methods_from_excel(excel_file_path)
        if not methods_data:
            logging.warning("No valid methods data extracted from the Excel file. Aborting.")
            return

        # Step 3: Ensure the DECODE project exists in the database
        project = session.query(Project).filter_by(name="DECODE").first()
        if not project:
            project = Project(name="DECODE")
            session.add(project)
            session.commit()
            logging.info("Created DECODE project in the database.")

        # Step 4: Ensure tasks exist in the database
        task_codes = set(method["task_code"] for method in methods_data)
        existing_tasks = {task.task_code: task for task in session.query(Task).all()}

        # Create missing tasks
        for task_code in task_codes:
            if task_code not in existing_tasks:
                new_task = Task(
                    project_id=project.project_id,
                    task_code=task_code,
                    description=f"Task {task_code} description"
                )
                session.add(new_task)
                session.commit()
                existing_tasks[task_code] = new_task
                logging.info(f"Created new task: {task_code}")

        # Step 5: Insert or update methods
        existing_methods = {method.name: method for method in session.query(Method).all()}

        for method_data in methods_data:
            task = existing_tasks.get(method_data["task_code"])
            if not task:
                logging.warning(f"Task {method_data['task_code']} not found. Skipping method: {method_data['name']}")
                continue

            method_name = method_data["name"]
            if method_name in existing_methods:
                # Update existing method if needed
                existing_method = existing_methods[method_name]
                existing_method.method_type = method_data["method_type"]
                existing_method.maturity = method_data["maturity"]
                logging.info(f"Updated existing method: {method_name}")
            else:
                # Add new method
                new_method = Method(
                    task_id=task.task_id,
                    name=method_name,
                    method_type=method_data["method_type"],
                    maturity=method_data["maturity"],
                    unique_id=str(uuid.uuid4())
                )
                session.add(new_method)
                session.commit()
                logging.info(f"Added new method: {method_name}")

        # Step 6: Insert MethodTechnologyService records
        for method in session.query(Method).all():
            # Replace the below random values with actual logic to fetch from Excel if available
            new_service = MethodTechnologyService(
                method_id=method.method_id,
                technology_id=random.randint(1, 5),  # Replace with actual technology association
                maturity_min=random.uniform(1.0, 3.0),
                maturity_max=random.uniform(4.0, 6.0),
                cost_min=random.uniform(1000.0, 5000.0),
                cost_max=random.uniform(6000.0, 10000.0),
                interoperability_min=random.uniform(1.0, 3.0),
                interoperability_max=random.uniform(4.0, 6.0),
                integration_min=random.uniform(1.0, 3.0),
                integration_max=random.uniform(4.0, 6.0),
            )
            session.add(new_service)
            logging.info(f"Added service for Method ID: {method.method_id}")

        # Commit all changes to the database
        session.commit()
        logging.info("Database population completed successfully.")

    except IntegrityError as ie:
        session.rollback()
        logging.error(f"Integrity error during database population: {ie}")
    except Exception as e:
        session.rollback()
        logging.error(f"Unexpected error during database population: {e}")
    finally:
        session.close()
        logging.info("Database session closed.")
