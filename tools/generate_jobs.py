from faker import Faker
import uuid
import random
from datetime import datetime
from logger import log_to_elasticsearch

# === Faker & données statiques ===
fake = Faker("fr_FR")

job_titles = [
    "Data Scientist", "Développeur Fullstack", "DevOps Engineer",
    "Chef de projet IT", "Consultant Data", "Product Manager",
    "Ingénieur IA", "Analyste cybersécurité", "UX Designer", "QA Engineer"
]

companies = [
    "Capgemini", "Dassault Systèmes", "Atos", "Thales", "Airbus",
    "Sopra Steria", "Ubisoft", "L'Oréal", "Sanofi", "BNP Paribas"
]

contract_types = ["CDI", "CDD", "Stage", "Alternance", "Freelance"]
locations = ["Paris", "Lyon", "Toulouse", "Nantes", "Lille", "Marseille"]

# === Connexion MySQL (base "jobs") ===
conn = mysql.connector.connect(
    host="mysql_jobs",  # ou localhost si local
    user="root",
    password="root",
    database="jobs"
)
cursor = conn.cursor()

# === Helper insertion ===
def insert(query, values):
    cursor.executemany(query, values)

@log_to_elasticsearch(index="log-init")
def generate_job():
    job_id = uuid.uuid4()
    title = random.choice(job_titles)
    company = random.choice(companies)
    location = random.choice(locations)
    contract = random.choice(contract_types)
    posted_at = fake.date_between(start_date="-30d", end_date="today").strftime("%Y-%m-%d %H:%M:%S")

    salary_min = random.randint(28000, 45000)
    salary_max = salary_min + random.randint(5000, 20000)

    remote_possible = random.choice([True, False])
    contact_email = fake.company_email()
    recruiter_name = fake.name()
    description = fake.paragraph(nb_sentences=5)
    requirements = fake.paragraph(nb_sentences=4)
    responsibilities = fake.paragraph(nb_sentences=4)
    company_website = fake.url()
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return (
        job_id.bytes, title, company, location, contract, posted_at,
        salary_min, salary_max, remote_possible, contact_email,
        recruiter_name, description, requirements, responsibilities,
        company_website, created_at
    )

# === Génération et insertion ===
jobs = [generate_job() for _ in range(500)]

insert("""
INSERT INTO jobs (
    id, title, company, location, contract_type, posted_at,
    salary_min, salary_max, remote_possible, contact_email,
    recruiter_name, description, requirements, responsibilities,
    company_website, created_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", jobs)

conn.commit()
logging.info("✅ 500 job offers generated and inserted.")

cursor.close()
conn.close()
