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
    "password": "",  # Update with your password
    "database": "dlh_data_warehouse",  # Menggunakan nama database baru untuk skema baru
}


class TranscriptETL:
    """Main ETL class for processing academic transcripts"""

    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.connection = None
        self.grade_to_bobot = {}  # To store grade to bobot mapping

    def connect_db(self) -> bool:
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            logger.info("Successfully connected to the database.")
            return True
        except mysql.connector.Error as err:
            logger.error(f"Could not connect to the database: {err}")
            return False

    def create_warehouse_schema(self):
        """Create the star schema tables based on the new diagram"""
        if not self.connection:
            logger.error("Database connection not found.")
            return False

        cursor = self.connection.cursor()

        try:
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS {self.db_config['database']}"
            )
            cursor.execute(f"USE {self.db_config['database']}")

            # SQL DDL disesuaikan dengan skema baru
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
                """CREATE TABLE IF NOT EXISTS Fact_History_Semester (
                    id_history INT AUTO_INCREMENT PRIMARY KEY,
                    id_mahasiswa INT NOT NULL,
                    id_waktu INT NOT NULL,
                    ips_semester DECIMAL(3,2),
                    ipk_semester DECIMAL(3,2),
                    jumlah_sks_semester INT,
                    FOREIGN KEY (id_mahasiswa) REFERENCES Dim_Mahasiswa(id_mahasiswa),
                    FOREIGN KEY (id_waktu) REFERENCES Dim_Waktu(id_waktu),
                    UNIQUE KEY unique_semester_history (id_mahasiswa, id_waktu)
                );""",
            ]

            for sql in table_sql:
                cursor.execute(sql)

            logger.info("New schema tables have been created.")

            self.connection.commit()
            logger.info("Schema setup is complete.")

            self._insert_reference_data()

        except mysql.connector.Error as err:
            logger.error(f"Failed to create schema: {err}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()

        return True

    def _insert_reference_data(self):
        """Insert reference data for grades and fetch into memory"""
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
                logger.info("Initial grade data has been inserted into Dim_Nilai.")

            # Fetch grades into memory for IPS/IPK calculation
            cursor.execute("SELECT huruf_nilai, bobot_nilai FROM Dim_Nilai")
            for row in cursor.fetchall():
                self.grade_to_bobot[row[0]] = float(row[1])
            logger.info("Loaded grade-to-weight mapping into memory.")

            self.connection.commit()

        except mysql.connector.Error as err:
            logger.error(f"Problem with reference data insertion/retrieval: {err}")
            self.connection.rollback()
        finally:
            cursor.close()

    def extract_pdf_text(self, pdf_path: str) -> str:
        """Extract text from PDF file and apply advanced cleaning."""
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
                    f"Text extracted and cleaned from {os.path.basename(pdf_path)}."
                )
                return full_text
        except Exception as e:
            logger.error(f"Could not read PDF file {pdf_path}: {e}")
            return ""

    def parse_transcript(self, text: str) -> Optional[Dict]:
        """Parse transcript text and extract structured data"""
        try:
            data = {
                "student": {},
                "courses": [],
                "semester_history": [],
            }  # Add semester_history

            student_info = self._parse_student_info(text)
            if not student_info:
                logger.error("Could not parse student details.")
                return None
            data["student"] = student_info

            courses = self._parse_courses(text)
            if not courses:
                logger.error("Could not parse course details.")
                return None
            data["courses"] = courses

            # Calculate semester history after parsing all courses
            data["semester_history"] = self._calculate_semester_history(courses)
            if not data["semester_history"]:
                logger.warning("Semester history could not be calculated.")

            logger.info(
                f"Transcript for {student_info['nama_mahasiswa']} parsed successfully."
            )
            return data

        except Exception as e:
            logger.error(f"An error occurred during transcript parsing: {e}")
            return None

    def _parse_student_info(self, text: str) -> Optional[Dict]:
        """Parse student information from the cleaned transcript text"""
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
                logger.error("Missing required fields in student header.")
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
            logger.error(f"Problem parsing student information: {e}")
            return None

    def _parse_courses(self, text: str) -> List[Dict]:
        """Parse course information from the cleaned transcript text"""
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

            logger.info(f"Found and parsed {len(courses)} courses.")
            return courses
        except Exception as e:
            logger.error(f"Problem parsing course data: {e}")
            return []

    def _calculate_semester_history(self, courses: List[Dict]) -> List[Dict]:
        """Calculates IPS, IPK, and total SKS for each semester."""
        semester_data = (
            {}
        )  # Key: (year, semester_name), Value: {'sks_semester': X, 'weighted_sks_semester': Y}

        # Sort courses chronologically for cumulative IPK calculation
        # 'Gasal' comes before 'Genap'
        sorted_courses = sorted(
            courses, key=lambda c: (c["tahun"], 0 if c["semester"] == "Gasal" else 1)
        )

        # First pass to aggregate data per semester
        for course in sorted_courses:
            year = course["tahun"]
            semester_name = course["semester"]
            sks_mk = course["sks_mk"]
            huruf_nilai = course["huruf_nilai"]

            bobot_nilai = self.grade_to_bobot.get(huruf_nilai)
            if bobot_nilai is None:
                logger.warning(
                    f"Weight for grade '{huruf_nilai}' is missing. Skipping {course['kode_mk']} in semester history calculation."
                )
                continue

            weighted_sks = sks_mk * bobot_nilai

            if (year, semester_name) not in semester_data:
                semester_data[(year, semester_name)] = {
                    "sks_semester": 0,
                    "weighted_sks_semester": 0,
                }

            semester_data[(year, semester_name)]["sks_semester"] += sks_mk
            semester_data[(year, semester_name)][
                "weighted_sks_semester"
            ] += weighted_sks

        # Second pass to calculate IPS and cumulative IPK
        history_list = []
        cumulative_sks_overall = 0
        cumulative_weighted_sks_overall = 0

        # Ensure semesters are processed in chronological order
        sorted_semester_keys = sorted(
            semester_data.keys(), key=lambda x: (x[0], 0 if x[1] == "Gasal" else 1)
        )

        for year, semester_name in sorted_semester_keys:
            sks_sem = semester_data[(year, semester_name)]["sks_semester"]
            weighted_sks_sem = semester_data[(year, semester_name)][
                "weighted_sks_semester"
            ]

            ips_semester = weighted_sks_sem / sks_sem if sks_sem > 0 else 0.0

            cumulative_sks_overall += sks_sem
            cumulative_weighted_sks_overall += weighted_sks_sem

            ipk_semester = (
                cumulative_weighted_sks_overall / cumulative_sks_overall
                if cumulative_sks_overall > 0
                else 0.0
            )

            history_list.append(
                {
                    "tahun": year,
                    "semester": semester_name,
                    "ips_semester": round(ips_semester, 2),
                    "ipk_semester": round(ipk_semester, 2),
                    "jumlah_sks_semester": sks_sem,
                }
            )

        logger.info(
            f"Finished calculating {len(history_list)} semester history records."
        )
        return history_list

    def load_to_warehouse(self, data: Dict) -> bool:
        """Load parsed data into the new data warehouse schema"""
        if not self.connection:
            logger.error("Cannot load data, no database connection.")
            return False

        cursor = self.connection.cursor(dictionary=True)

        try:
            id_mahasiswa = self._load_mahasiswa(cursor, data["student"])
            if not id_mahasiswa:
                return False

            # Load course facts
            success_count_courses = 0
            for course in data["courses"]:
                if self._load_course_fact(cursor, id_mahasiswa, course):
                    success_count_courses += 1

            # Load semester history facts
            history_success_count = 0
            for semester_entry in data["semester_history"]:
                if self._load_history_semester(cursor, id_mahasiswa, semester_entry):
                    history_success_count += 1

            self.connection.commit()
            logger.info(
                f"Loaded {success_count_courses} of {len(data['courses'])} courses for {data['student']['nama_mahasiswa']}."
            )
            logger.info(
                f"Loaded {history_success_count} of {len(data['semester_history'])} semester records for {data['student']['nama_mahasiswa']}."
            )
            return True
        except mysql.connector.Error as err:
            logger.error(f"A database error occurred while loading: {err}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()

    def _load_mahasiswa(self, cursor, student_data: Dict) -> Optional[int]:
        """Load or update student data and return student key"""
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
                logger.info(
                    f"Student record updated for: {student_data['nama_mahasiswa']}"
                )
            else:
                insert_sql = """
                    INSERT INTO Dim_Mahasiswa (
                        NRP, nama_mahasiswa, status_mahasiswa, ipk_kumulatif, sks_tempuh, 
                        sks_lulus, ip_persiapan, sks_persiapan, ip_sarjana, sks_sarjana
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_sql, (student_data["nrp"],) + student_columns)
                id_mahasiswa = cursor.lastrowid
                logger.info(
                    f"New student record created for: {student_data['nama_mahasiswa']}"
                )

            return id_mahasiswa
        except mysql.connector.Error as err:
            logger.error(f"Failed to load student data: {err}")
            return None

    def _load_course_fact(self, cursor, id_mahasiswa: int, course_data: Dict) -> bool:
        """Load course, time, grade dimensions and the transcript fact"""
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
                    f"Could not find grade '{course_data['huruf_nilai']}' in Dim_Nilai. Skipping fact record for {course_data['kode_mk']}."
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
                logger.debug(
                    f"Fact record created for course: {course_data['kode_mk']}"
                )
            return True
        except (mysql.connector.Error, TypeError) as err:
            logger.error(
                f"Could not load course fact for '{course_data['kode_mk']}': {err}"
            )
            return False

    def _load_history_semester(
        self, cursor, id_mahasiswa: int, semester_entry: Dict
    ) -> bool:
        """Load semester history data into Fact_History_Semester"""
        try:
            id_waktu = self._get_or_create_key(
                cursor,
                "Dim_Waktu",
                "id_waktu",
                "tahun = %s AND semester = %s",
                (semester_entry["tahun"], semester_entry["semester"]),
                "INSERT INTO Dim_Waktu (tahun, semester) VALUES (%s, %s)",
                (semester_entry["tahun"], semester_entry["semester"]),
            )

            if not id_waktu:
                logger.error(
                    f"Failed to retrieve or create time ID for {semester_entry['tahun']}/{semester_entry['semester']}"
                )
                return False

            cursor.execute(
                "SELECT id_history FROM Fact_History_Semester WHERE id_mahasiswa = %s AND id_waktu = %s",
                (id_mahasiswa, id_waktu),
            )

            if not cursor.fetchone():
                insert_sql = """
                    INSERT INTO Fact_History_Semester (
                        id_mahasiswa, id_waktu, ips_semester, ipk_semester, jumlah_sks_semester
                    ) VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(
                    insert_sql,
                    (
                        id_mahasiswa,
                        id_waktu,
                        semester_entry["ips_semester"],
                        semester_entry["ipk_semester"],
                        semester_entry["jumlah_sks_semester"],
                    ),
                )
                logger.debug(
                    f"New semester history added for student {id_mahasiswa} ({semester_entry['tahun']}/{semester_entry['semester']})"
                )
            else:
                update_sql = """
                    UPDATE Fact_History_Semester SET
                        ips_semester = %s, ipk_semester = %s, jumlah_sks_semester = %s
                    WHERE id_mahasiswa = %s AND id_waktu = %s
                """
                cursor.execute(
                    update_sql,
                    (
                        semester_entry["ips_semester"],
                        semester_entry["ipk_semester"],
                        semester_entry["jumlah_sks_semester"],
                        id_mahasiswa,
                        id_waktu,
                    ),
                )
                logger.debug(
                    f"Semester history updated for student {id_mahasiswa} ({semester_entry['tahun']}/{semester_entry['semester']})"
                )
            return True
        except mysql.connector.Error as err:
            logger.error(
                f"Could not load semester history for student {id_mahasiswa} ({semester_entry['tahun']}/{semester_entry['semester']}): {err}"
            )
            return False

    def _get_or_create_key(
        self, cursor, table, key_col, where_col, where_val, insert_sql, insert_val
    ) -> Optional[int]:
        """Generic function to get or create a dimension key."""
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
            logger.error(f"Problem with dimension {table}: {err}")
            return None

    def process_folder(self, folder_path: str) -> Dict[str, int]:
        """Process all PDF files in a folder"""
        if not os.path.isdir(folder_path):
            logger.error(f"Directory does not exist: {folder_path}")
            return {"processed": 0, "failed": 0}

        stats = {"processed": 0, "failed": 0}

        pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".pdf")]
        logger.info(f"Located {len(pdf_files)} PDF files for processing.")

        for filename in pdf_files:
            pdf_path = os.path.join(folder_path, filename)
            logger.info(f"Now processing: {filename}")

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
                    logger.info(f"Finished processing: {filename}")
                else:
                    stats["failed"] += 1
                    logger.error(f"Could not load data from: {filename}")

            except Exception as e:
                logger.error(f"An unexpected error occurred with {filename}: {e}")
                stats["failed"] += 1

        return stats

    def close_connection(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Connection to database has been closed.")


def main():
    """Main execution function"""
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
                f"'{folder_path}' folder has been created. Put PDF files here to get started."
            )
            return

        logger.info("Kicking off transcript processing...")
        stats = etl.process_folder(folder_path)

        logger.info("=" * 60)
        logger.info("ALL TASKS FINISHED")
        logger.info(f"Files processed successfully: {stats['processed']}")
        logger.info(f"Files that failed processing: {stats['failed']}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"A critical error happened during main execution: {e}")
    finally:
        etl.close_connection()


if __name__ == "__main__":
    main()
