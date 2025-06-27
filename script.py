import os
import re
import logging
import pandas as pd
import mysql.connector
from PyPDF2 import PdfReader
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("transcript_etl.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Database Configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "dlh_transcript",
}


class TranscriptETL:
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.connection = None

    def connect_db(self) -> bool:
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            logger.info("✅ --- Connection to the database was successful. --- ✅")
            return True
        except mysql.connector.Error as err:
            logger.error(f"🚨 --- Could not connect to the database: {err} --- 🚨")
            return False

    def create_warehouse_schema(self):
        """Create the star schema tables based on the new diagram"""
        if not self.connection:
            logger.error("Database connection is not available. Cannot create schema.")
            return False

        cursor = self.connection.cursor()

        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_config['database']}")
        cursor.execute(f"USE {self.db_config['database']}")

        try:
            # SQL DDL
            table_sql = [
                """CREATE TABLE IF NOT EXISTS Dim_Mahasiswa (
                    id_mahasiswa INT AUTO_INCREMENT PRIMARY KEY,
                    NRP VARCHAR(20) UNIQUE NOT NULL,
                    nama_mahasiswa VARCHAR(100) NOT NULL,
                    status_mahasiswa VARCHAR(50),
                    ipk_kumulatif DECIMAL(3,2),
                    sks_tempuh INT,
                    sks_lulus INT,
                    ip_persiapan DECIMAL(3,2),
                    sks_persiapan INT,
                    ip_sarjana DECIMAL(3,2),
                    sks_sarjana INT
                )""",
                """CREATE TABLE IF NOT EXISTS Dim_MataKuliah (
                    id_mk INT AUTO_INCREMENT PRIMARY KEY,
                    kode_mk VARCHAR(20) UNIQUE NOT NULL,
                    nama_mk VARCHAR(200) NOT NULL,
                    sks_mk INT NOT NULL,
                    tahap_mk VARCHAR(50) NOT NULL
                )""",
                """CREATE TABLE IF NOT EXISTS Dim_Waktu (
                    id_waktu INT AUTO_INCREMENT PRIMARY KEY,
                    tahun INT NOT NULL,
                    semester VARCHAR(20) NOT NULL,
                    UNIQUE KEY unique_time (tahun, semester)
                )""",
                """CREATE TABLE IF NOT EXISTS Dim_Nilai (
                    id_nilai INT AUTO_INCREMENT PRIMARY KEY,
                    huruf_nilai VARCHAR(5) UNIQUE NOT NULL,
                    bobot_nilai DECIMAL(3,2) NOT NULL
                )""",
                """CREATE TABLE IF NOT EXISTS Fact_Transkrip (
                    id_transkrip INT AUTO_INCREMENT PRIMARY KEY,
                    id_mahasiswa INT NOT NULL,
                    id_mk INT NOT NULL,
                    id_waktu INT NOT NULL,
                    id_nilai INT NOT NULL,
                    bobot_matkul DECIMAL(4,2) NOT NULL,
                    FOREIGN KEY (id_mahasiswa) REFERENCES Dim_Mahasiswa(id_mahasiswa),
                    FOREIGN KEY (id_mk) REFERENCES Dim_MataKuliah(id_mk),
                    FOREIGN KEY (id_waktu) REFERENCES Dim_Waktu(id_waktu),
                    FOREIGN KEY (id_nilai) REFERENCES Dim_Nilai(id_nilai),
                    UNIQUE KEY unique_transcript (id_mahasiswa, id_mk, id_waktu)
                )""",
            ]

            for sql in table_sql:
                cursor.execute(sql)

            logger.info("✨ --- All tables have been created according to the schema. --- ✨")

            self.connection.commit()
            logger.info("🚀 --- Database schema setup is fully complete. --- 🚀")

            self._insert_reference_data()

        except mysql.connector.Error as err:
            logger.error(f"Schema creation failed with error: {err}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()

        return True

    def _insert_reference_data(self):
        cursor = self.connection.cursor()

        try:
            grades = [
                ("A", 4.0),
                ("AB", 3.5),
                ("B", 3.0),
                ("BC", 2.5),
                ("C", 2.0),
                ("D", 1.0),
                ("E", 0.0),
            ]
            cursor.execute("SELECT COUNT(*) FROM Dim_Nilai")
            if cursor.fetchone()[0] == 0:
                cursor.executemany(
                    "INSERT INTO Dim_Nilai (huruf_nilai, bobot_nilai) VALUES (%s, %s)",
                    grades,
                )
                logger.info("Initial grade data has been populated in Dim_Nilai.")

            self.connection.commit()

        except mysql.connector.Error as err:
            logger.error(f"🚨 --- Failed to insert reference data: {err} --- 🚨")
            self.connection.rollback()
        finally:
            cursor.close()

    def extract_pdf_text(self, pdf_path: str) -> str:
        try:
            with open(pdf_path, "rb") as file:
                reader = PdfReader(file)
                full_text = ""
                for page in reader.pages:
                    page_text = page.extract_text() or ""
                    cleaned_text = re.sub(r"\b([A-Z])\s([a-z])", r"\1\2", page_text)
                    cleaned_text = re.sub(r"([a-z])([A-Z])", r"\1 \2", cleaned_text)
                    cleaned_text = re.sub(r"\s+", " ", cleaned_text).strip()
                    full_text += cleaned_text + "\n"

                logger.info(
                    f"📄 --- Text extraction and cleanup finished for {os.path.basename(pdf_path)}. --- 📄"
                )
                return full_text
        except Exception as e:
            logger.error(f"🛑 --- Problem reading the PDF file {pdf_path}: {e} --- 🛑")
            return ""

    def parse_transcript(self, text: str) -> Optional[Dict]:
        try:
            data = {"student": {}, "courses": []}

            student_info = self._parse_student_info(text)
            if not student_info:
                logger.error("🚨 --- Student information could not be parsed. --- 🚨")
                return None
            data["student"] = student_info

            courses = self._parse_courses(text)
            if not courses:
                logger.error("🚨 --- Course information could not be parsed. --- 🚨")
                return None
            data["courses"] = courses

            logger.info(
                f"✅ --- Transcript for {student_info['nama_mahasiswa']} has been parsed successfully. --- ✅"
            )
            return data

        except Exception as e:
            logger.error(f"🛑 --- An issue occurred while parsing the transcript: {e} --- 🛑")
            return None

    def _parse_student_info(self, text: str) -> Optional[Dict]:
        try:
            nrp_nama_match = re.search(
                r"NRP\s*/\s*Nama\s*(\d+)\s*/\s*(.*?)\s*SKS Tempuh", text, re.DOTALL
            )
            sks_match = re.search(
                r"SKS\s*Tempuh\s*/\s*SKS\s*Lulus\s*(\d+)\s*/\s*(\d+)", text
            )
            status_match = re.search(r"Status\s*(.*?)(?=\s*Tahap|---)", text, re.DOTALL)
            ipk_match = re.search(r"IPK\s*([\d.]+)", text)

            ip_persiapan_match = re.search(
                r"IP Tahap Persiapan\s*:\s*([\d.]+)", text, re.IGNORECASE
            )
            sks_persiapan_match = re.search(
                r"Total Sks Tahap Persiapan\s*:\s*(\d+)", text, re.IGNORECASE
            )
            ip_sarjana_match = re.search(
                r"IP Tahap Sarjana\s*:\s*([\d.]+)", text, re.IGNORECASE
            )
            sks_sarjana_match = re.search(
                r"Total Sks Tahap Sarjana\s*:\s*(\d+)", text, re.IGNORECASE
            )

            if not all([nrp_nama_match, sks_match, status_match, ipk_match]):
                logger.error("Essential student header information is missing.")
                return None

            nama = re.sub(r"\s+", " ", nrp_nama_match.group(2)).strip()
            status = re.sub(r"\s+", " ", status_match.group(1)).strip()

            return {
                "nrp": nrp_nama_match.group(1).strip(),
                "nama_mahasiswa": nama,
                "status_mahasiswa": status,
                "sks_tempuh": int(sks_match.group(1)),
                "sks_lulus": int(sks_match.group(2)),
                "ipk": float(ipk_match.group(1)),
                "ip_persiapan": (
                    float(ip_persiapan_match.group(1)) if ip_persiapan_match else 0.0
                ),
                "sks_persiapan": (
                    int(sks_persiapan_match.group(1)) if sks_persiapan_match else 0
                ),
                "ip_sarjana": (
                    float(ip_sarjana_match.group(1)) if ip_sarjana_match else 0.0
                ),
                "sks_sarjana": (
                    int(sks_sarjana_match.group(1)) if sks_sarjana_match else 0
                ),
            }
        except Exception as e:
            logger.error(f"🛑 --- Failed to parse student details: {e} --- 🛑")
            return None

    def _parse_courses(self, text: str) -> List[Dict]:
        try:
            courses = []

            course_pattern = r"([A-Z]{2}\d{5,6})\s*(.*?)\s*(\d)\s*(\d{4}/(?:Gs|Gn)/[A-Z]{1,2})\s*([A-Z]{1,2})"

            matches = re.finditer(course_pattern, text, re.DOTALL)

            sarjana_start_match = re.search(r"Tahap:\s*Sarjana", text)
            sarjana_start_pos = (
                sarjana_start_match.start() if sarjana_start_match else -1
            )

            for match in matches:
                course_name = re.sub(r"\s+", " ", match.group(2)).strip()
                sks = int(match.group(3))
                hist_info = match.group(4)
                grade = match.group(5)

                year_sem_match = re.search(r"(\d{4})/(Gs|Gn)", hist_info)
                if not year_sem_match:
                    continue

                year, sem_code = year_sem_match.groups()

                phase = (
                    "Sarjana"
                    if sarjana_start_pos != -1 and match.start() > sarjana_start_pos
                    else "Persiapan"
                )
                semester = "Gasal" if sem_code == "Gs" else "Genap"

                courses.append(
                    {
                        "kode_mk": match.group(1).strip(),
                        "nama_mk": course_name,
                        "sks_mk": sks,
                        "tahun": int(year),
                        "semester": semester,
                        "huruf_nilai": grade.strip(),
                        "tahap_mk": phase,
                    }
                )

            logger.info(f"Identified and parsed {len(courses)} courses from the text.")
            return courses
        except Exception as e:
            logger.error(f"An error occurred while parsing courses: {e}")
            return []

    def load_to_warehouse(self, data: Dict) -> bool:
        if not self.connection:
            logger.error("Cannot load data; no active database connection.")
            return False

        cursor = self.connection.cursor(dictionary=True)

        try:
            id_mahasiswa = self._load_mahasiswa(cursor, data["student"])
            if not id_mahasiswa:
                return False

            success_count = 0
            for course in data["courses"]:
                if self._load_course_fact(cursor, id_mahasiswa, course):
                    success_count += 1

            self.connection.commit()
            logger.info(
                f"✅ --- Load complete: {success_count}/{len(data['courses'])} course records for {data['student']['nama_mahasiswa']} were loaded. --- ✅"
            )
            return True
        except mysql.connector.Error as err:
            logger.error(f"A database error occurred during the loading process: {err}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()

    def _load_mahasiswa(self, cursor, student_data: Dict) -> Optional[int]:
        try:
            cursor.execute(
                "SELECT id_mahasiswa FROM Dim_Mahasiswa WHERE NRP = %s",
                (student_data["nrp"],),
            )
            result = cursor.fetchone()

            student_columns = (
                student_data["nama_mahasiswa"],
                student_data["status_mahasiswa"],
                student_data["ipk"],
                student_data["sks_tempuh"],
                student_data["sks_lulus"],
                student_data["ip_persiapan"],
                student_data["sks_persiapan"],
                student_data["ip_sarjana"],
                student_data["sks_sarjana"],
            )

            if result:
                id_mahasiswa = result["id_mahasiswa"]
                update_sql = """
                    UPDATE Dim_Mahasiswa SET 
                        nama_mahasiswa = %s, status_mahasiswa = %s, ipk_kumulatif = %s,
                        sks_tempuh = %s, sks_lulus = %s, ip_persiapan = %s, sks_persiapan = %s,
                        ip_sarjana = %s, sks_sarjana = %s
                    WHERE id_mahasiswa = %s
                """
                cursor.execute(update_sql, student_columns + (id_mahasiswa,))
                logger.info(f"Student record updated for: {student_data['nama_mahasiswa']}")
            else:
                insert_sql = """
                    INSERT INTO Dim_Mahasiswa (
                        NRP, nama_mahasiswa, status_mahasiswa, ipk_kumulatif, sks_tempuh, 
                        sks_lulus, ip_persiapan, sks_persiapan, ip_sarjana, sks_sarjana
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_sql, (student_data["nrp"],) + student_columns)
                id_mahasiswa = cursor.lastrowid
                logger.info(f"New student record created for: {student_data['nama_mahasiswa']}")

            return id_mahasiswa
        except mysql.connector.Error as err:
            logger.error(f"Failed to load student data: {err}")
            return None

    def _load_course_fact(self, cursor, id_mahasiswa: int, course_data: Dict) -> bool:
        try:
            id_mk = self._get_or_create_key(
                cursor,
                "Dim_MataKuliah",
                "id_mk",
                "kode_mk",
                course_data["kode_mk"],
                "INSERT INTO Dim_MataKuliah (kode_mk, nama_mk, sks_mk, tahap_mk) VALUES (%s, %s, %s, %s)",
                (
                    course_data["kode_mk"],
                    course_data["nama_mk"],
                    course_data["sks_mk"],
                    course_data["tahap_mk"],
                ),
            )

            id_waktu = self._get_or_create_key(
                cursor,
                "Dim_Waktu",
                "id_waktu",
                "tahun = %s AND semester = %s",
                (course_data["tahun"], course_data["semester"]),
                "INSERT INTO Dim_Waktu (tahun, semester) VALUES (%s, %s)",
                (course_data["tahun"], course_data["semester"]),
            )

            cursor.execute(
                "SELECT id_nilai, bobot_nilai FROM Dim_Nilai WHERE huruf_nilai = %s",
                (course_data["huruf_nilai"],),
            )
            nilai_result = cursor.fetchone()
            if not nilai_result:
                logger.warning(
                    f"Grade '{course_data['huruf_nilai']}' is not defined in Dim_Nilai. Omitting fact for {course_data['kode_mk']}."
                )
                return False
            id_nilai = nilai_result["id_nilai"]
            bobot_nilai = nilai_result["bobot_nilai"]

            bobot_matkul = course_data["sks_mk"] * bobot_nilai

            cursor.execute(
                "SELECT id_transkrip FROM Fact_Transkrip WHERE id_mahasiswa = %s AND id_mk = %s AND id_waktu = %s",
                (id_mahasiswa, id_mk, id_waktu),
            )

            if not cursor.fetchone():
                cursor.execute(
                    """INSERT INTO Fact_Transkrip 
                    (id_mahasiswa, id_mk, id_waktu, id_nilai, bobot_matkul) 
                    VALUES (%s, %s, %s, %s, %s)""",
                    (id_mahasiswa, id_mk, id_waktu, id_nilai, bobot_matkul),
                )
                logger.debug(f"Fact record inserted for course: {course_data['kode_mk']}")
            return True
        except (mysql.connector.Error, TypeError) as err:
            logger.error(
                f"Could not load course fact for '{course_data['kode_mk']}': {err}"
            )
            return False

    def _get_or_create_key(
        self, cursor, table, key_col, where_col, where_val, insert_sql, insert_val
    ) -> Optional[int]:
        try:
            if isinstance(where_val, tuple):
                query = f"SELECT {key_col} FROM {table} WHERE {where_col}"
                cursor.execute(query, where_val)
            else:
                query = f"SELECT {key_col} FROM {table} WHERE {where_col} = %s"
                cursor.execute(query, (where_val,))

            result = cursor.fetchone()

            if result:
                return result[key_col]
            else:
                cursor.execute(insert_sql, insert_val)
                return cursor.lastrowid
        except mysql.connector.Error as err:
            logger.error(f"Problem accessing dimension {table}: {err}")
            return None

    def process_folder(self, folder_path: str) -> Dict[str, int]:
        if not os.path.isdir(folder_path):
            logger.error(f"The specified folder does not exist: {folder_path}")
            return {"processed": 0, "failed": 0}

        stats = {"processed": 0, "failed": 0}

        pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".pdf")]
        logger.info(f"Discovered {len(pdf_files)} PDF files to be processed.")

        for filename in pdf_files:
            pdf_path = os.path.join(folder_path, filename)
            logger.info(f"Now processing file: {filename}")

            try:
                text = self.extract_pdf_text(pdf_path)
                if not text:
                    stats["failed"] += 1
                    continue

                data = self.parse_transcript(text)
                if not data:
                    stats["failed"] += 1
                    continue

                if self.load_to_warehouse(data):
                    stats["processed"] += 1
                    logger.info(f"✅ --- Successfully processed and loaded: {filename} --- ✅")
                else:
                    stats["failed"] += 1
                    logger.error(f"🚨 --- Failed to load data from: {filename} --- 🚨")

            except Exception as e:
                logger.error(f"🛑 --- A critical error occurred while handling {filename}: {e} --- 🛑")
                stats["failed"] += 1

        return stats

    def close_connection(self):
        if self.connection:
            self.connection.close()
            logger.info("Database connection has been terminated.")


def main():
    etl = TranscriptETL(DB_CONFIG)

    try:
        if not etl.connect_db():
            return

        if not etl.create_warehouse_schema():
            return

        folder_path = "transkrip/"

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            logger.info(
                f"The input folder '{folder_path}' was not found and has been created. Please add PDF transcripts to it."
            )
            return

        logger.info("Beginning the transcript processing workflow...")
        stats = etl.process_folder(folder_path)

        logger.info("=" * 60)
        logger.info("WORKFLOW COMPLETED")
        logger.info(f"✅ --- Total files successfully processed: {stats['processed']} --- ✅")
        logger.info(f"🚨 --- Total files that failed processing: {stats['failed']} --- 🚨")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"An unexpected top-level error occurred: {e}")
    finally:
        etl.close_connection()


if __name__ == "__main__":
    main()
    