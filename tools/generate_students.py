from faker import Faker
import uuid
import random
from datetime import datetime, timedelta
import mysql.connector

# === Faker & Static data ===
fake = Faker("fr_FR")

fields = ["Informatique", "Data Science", "IA", "Sécurité", "Marketing"]
levels = ["Licence", "Master", "Doctorat"]
tags = ["motivé", "curieux", "autonome", "organisé"]
positions = ["Data Scientist", "Dev Fullstack", "PM", "Product Owner"]
schools = ["HEC", "Polytechnique", "CentraleSupélec", "INSA", "UTC"]
languages = ["Français", "Anglais", "Espagnol", "Allemand"]
skills = ["Python", "SQL", "Linux", "Java", "Docker"]
cities = ["Paris", "Lyon", "Marseille", "Toulouse", "Nantes"]

# === DB connection ===
conn = mysql.connector.connect(
    host="mysql_students",
    user="root",
    password="root",
    database="students"
)
cursor = conn.cursor()

def insert(query, values):
    cursor.executemany(query, values)

@log_to_elasticsearch(index="log-init")
def generate_student():
    student_id = uuid.uuid4()
    fname = fake.first_name()
    lname = fake.last_name()
    email = fake.email()
    phone = fake.phone_number()
    gender = random.choice(["M", "F", "Other"])
    nationality = fake.country()
    birth_date = fake.date_of_birth(minimum_age=20, maximum_age=30).strftime("%Y-%m-%d")
    address = fake.street_address()
    city = fake.city()
    postal_code = fake.postcode()
    country = "France"
    lat = round(fake.latitude(), 6)
    lon = round(fake.longitude(), 6)

    level = random.choice(levels)
    field = random.choice(fields)
    grad_year = random.randint(2023, 2026)

    gpa = round(random.uniform(2.5, 4.0), 2)
    program_name = f"{level} en {field}"
    school_website = fake.url()

    linkedin = f"https://linkedin.com/in/{fname.lower()}.{lname.lower()}"
    github = f"https://github.com/{fname.lower()}{random.randint(1,99)}"
    portfolio = fake.url()
    about_me = fake.text(max_nb_chars=150)
    preferred_salary = random.randint(30000, 60000)
    interests = ', '.join(fake.words(nb=random.randint(2, 5)))

    availability_date = fake.date_between(start_date="+0d", end_date="+90d").strftime("%Y-%m-%d")
    is_actively_looking = random.choice([True, False])
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    student_row = (
        student_id.bytes, fname, lname, email, phone, gender, nationality,
        birth_date, address, city, postal_code, country, lat, lon,
        level, field, grad_year, gpa, program_name, school_website,
        linkedin, github, portfolio, about_me, preferred_salary,
        interests, availability_date, is_actively_looking, created_at
    )

    lang_rows = [(student_id.bytes, random.choice(languages)) for _ in range(random.randint(1, 3))]
    skill_rows = [(student_id.bytes, random.choice(skills)) for _ in range(random.randint(3, 6))]
    tag_rows = [(student_id.bytes, random.choice(tags)) for _ in range(random.randint(1, 3))]
    pos_rows = [(student_id.bytes, random.choice(positions)) for _ in range(random.randint(1, 2))]
    edu_rows = [
        (
            student_id.bytes,
            random.choice(schools),
            random.choice(cities),
            random.randint(2016, 2020),
            random.randint(2021, 2024)
        ) for _ in range(random.randint(1, 2))
    ]

    return student_row, lang_rows, skill_rows, tag_rows, pos_rows, edu_rows

# === Main bulk insert ===
students = []
languages_data = []
skills_data = []
tags_data = []
positions_data = []
education_data = []

for _ in range(500):
    s, l, sk, t, p, e = generate_student()
    students.append(s)
    languages_data.extend(l)
    skills_data.extend(sk)
    tags_data.extend(t)
    positions_data.extend(p)
    education_data.extend(e)

# === SQL Insertions ===
insert("""
INSERT INTO students (
    id, first_name, last_name, email, phone, gender, nationality,
    birth_date, address, city, postal_code, country,
    gps_latitude, gps_longitude,
    level_of_study, field_of_study, graduation_year,
    gpa, program_name, school_website,
    linkedin_url, github_url, portfolio_url,
    about_me, preferred_salary, interests,
    availability_date, is_actively_looking, created_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", students)

insert("INSERT INTO student_languages (student_id, language) VALUES (%s, %s)", languages_data)
insert("INSERT INTO student_skills (student_id, skill) VALUES (%s, %s)", skills_data)
insert("INSERT INTO student_tags (student_id, tag) VALUES (%s, %s)", tags_data)
insert("INSERT INTO student_positions (student_id, position_title) VALUES (%s, %s)", positions_data)
insert("""
INSERT INTO student_education (student_id, school_name, city, start_year, end_year)
VALUES (%s, %s, %s, %s, %s)
""", education_data)

conn.commit()
print("✅ 500 students generated and inserted.")

cursor.close()
conn.close()
