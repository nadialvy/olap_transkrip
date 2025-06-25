-- Insight 1: Rata-Rata IPK Kelas
-- Menampilkan rata-rata IPK seluruh mahasiswa untuk melihat performa kelas secara umum.
SELECT
    AVG(ipk_kumulatif) AS rata_rata_ipk_seluruh_mahasiswa
FROM
    Dim_Mahasiswa;


-- Insight 2: Peringkat IPK Mahasiswa
-- Menampilkan daftar mahasiswa yang diurutkan berdasarkan IPK tertinggi.
SELECT
    NRP,
    nama_mahasiswa,
    ipk_kumulatif
FROM
    Dim_Mahasiswa
ORDER BY
    ipk_kumulatif DESC;


-- Insight 3: Rata-Rata IPS per Semester
-- Menganalisis tren performa akademik dengan menghitung rata-rata Indeks Prestasi Semester (IPS)
-- untuk setiap semester. IPS dihitung dengan formula: SUM(SKS * Bobot Nilai) / SUM(SKS).
SELECT
    w.tahun,
    w.semester,
    SUM(mk.sks_mk * n.bobot_nilai) / SUM(mk.sks_mk) AS rata_rata_ips
FROM
    Fact_Transkrip ft
JOIN Dim_MataKuliah mk ON ft.id_mk = mk.id_mk
JOIN Dim_Nilai n ON ft.id_nilai = n.id_nilai
JOIN Dim_Waktu w ON ft.id_waktu = w.id_waktu
GROUP BY
    w.tahun,
    w.semester
ORDER BY
    w.tahun,
    w.semester;


-- Insight 4: Perbandingan IP Tahap Persiapan vs Tahap Sarjana
-- Membandingkan rata-rata IP mahasiswa pada tahap persiapan dan tahap sarjana
-- untuk melihat peningkatan atau penurunan performa.
SELECT
    AVG(ip_persiapan) AS rata_rata_ip_persiapan,
    AVG(ip_sarjana) AS rata_rata_ip_sarjana
FROM
    Dim_Mahasiswa
WHERE
    ip_persiapan > 0 AND sks_persiapan > 0; -- Hanya hitung mahasiswa yg sudah melewati tahap persiapan


-- Insight 5: Top 5 Mata Kuliah dengan Nilai Rata-Rata Terendah
-- Mengidentifikasi mata kuliah yang paling menantang bagi mahasiswa berdasarkan rata-rata bobot nilai.
SELECT
    mk.nama_mk,
    mk.kode_mk,
    AVG(n.bobot_nilai) AS rata_rata_bobot_nilai
FROM
    Fact_Transkrip ft
JOIN Dim_MataKuliah mk ON ft.id_mk = mk.id_mk
JOIN Dim_Nilai n ON ft.id_nilai = n.id_nilai
GROUP BY
    mk.id_mk, mk.nama_mk, mk.kode_mk
ORDER BY
    rata_rata_bobot_nilai ASC
LIMIT 5;


-- Insight 6: Top 5 Mata Kuliah dengan Nilai Rata-Rata Tertinggi
-- Mengidentifikasi mata kuliah di mana mahasiswa menunjukkan performa terbaik.
SELECT
    mk.nama_mk,
    mk.kode_mk,
    AVG(n.bobot_nilai) AS rata_rata_bobot_nilai
FROM
    Fact_Transkrip ft
JOIN Dim_MataKuliah mk ON ft.id_mk = mk.id_mk
JOIN Dim_Nilai n ON ft.id_nilai = n.id_nilai
GROUP BY
    mk.id_mk, mk.nama_mk, mk.kode_mk
ORDER BY
    rata_rata_bobot_nilai DESC
LIMIT 5;


-- Insight 7: Mahasiswa yang Pernah Tidak Lulus & Tidak Mengulang
-- Menemukan mahasiswa yang mendapat nilai D atau E pada suatu mata kuliah dan tidak pernah
-- mengambil ulang mata kuliah tersebut untuk memperbaikinya.
SELECT DISTINCT
    m.NRP,
    m.nama_mahasiswa,
    mk.kode_mk,
    mk.nama_mk AS mata_kuliah_tidak_lulus
FROM
    Fact_Transkrip ft_gagal
JOIN Dim_Mahasiswa m ON ft_gagal.id_mahasiswa = m.id_mahasiswa
JOIN Dim_MataKuliah mk ON ft_gagal.id_mk = mk.id_mk
JOIN Dim_Nilai n_gagal ON ft_gagal.id_nilai = n_gagal.id_nilai
WHERE
    n_gagal.huruf_nilai IN ('D', 'E')
    AND NOT EXISTS (
        -- Subquery untuk mengecek apakah ada record lulus untuk mata kuliah yang sama
        SELECT 1
        FROM Fact_Transkrip ft_lulus
        JOIN Dim_Nilai n_lulus ON ft_lulus.id_nilai = n_lulus.id_nilai
        WHERE
            ft_lulus.id_mahasiswa = ft_gagal.id_mahasiswa
            AND ft_lulus.id_mk = ft_gagal.id_mk
            AND n_lulus.huruf_nilai NOT IN ('D', 'E')
    );


-- Insight 8: Top 5 IP Tahap Persiapan Tertinggi
-- Menampilkan 5 mahasiswa dengan performa terbaik selama tahap persiapan studi.
SELECT
    NRP,
    nama_mahasiswa,
    ip_persiapan
FROM
    Dim_Mahasiswa
ORDER BY
    ip_persiapan DESC
LIMIT 5;


-- Insight 9: Top 5 IP Tahap Sarjana Tertinggi
-- Menampilkan 5 mahasiswa dengan performa terbaik selama tahap sarjana.
SELECT
    NRP,
    nama_mahasiswa,
    ip_sarjana
FROM
    Dim_Mahasiswa
ORDER BY
    ip_sarjana DESC
LIMIT 5;

-- Insight 10: Jalur Masuk Setiap Mahasiswa
-- Menampilkan daftar setiap mahasiswa beserta jalur masuknya (SNBP, SNBT, Mandiri)
-- yang ditentukan secara dinamis berdasarkan rentang NRP.
SELECT
    m.NRP,
    m.nama_mahasiswa,
    CASE
        WHEN m.NRP BETWEEN '5026231001' AND '5026231042' THEN 'SNBP'
        WHEN m.NRP BETWEEN '5026231043' AND '5026231116' THEN 'SNBT'
        WHEN m.NRP BETWEEN '5026231117' AND '5026231232' THEN 'Mandiri'
        ELSE 'Lainnya' -- Untuk NRP di luar rentang yang ditentukan
    END AS jalur_masuk
FROM
    Dim_Mahasiswa m
ORDER BY
    m.NRP;


-- Insight 11: Perbandingan Rata-Rata IPK Berdasarkan Jalur Masuk (via NRP)
-- Query ini secara dinamis mengelompokkan mahasiswa ke dalam jalur masuk
-- menggunakan statement CASE WHEN berdasarkan rentang NRP mereka,
-- tanpa perlu mengubah struktur tabel.
SELECT
    jalur_masuk,
    AVG(ipk_kumulatif) AS rata_rata_ipk,
    COUNT(id_mahasiswa) AS jumlah_mahasiswa
FROM
    (
        SELECT
            id_mahasiswa,
            ipk_kumulatif,
            CASE
                WHEN NRP BETWEEN '5026231001' AND '5026231042' THEN 'SNBP'
                WHEN NRP BETWEEN '5026231043' AND '5026231116' THEN 'SNBT'
                WHEN NRP BETWEEN '5026231117' AND '5026231232' THEN 'Mandiri'
                ELSE NULL 
            END AS jalur_masuk
        FROM
            Dim_Mahasiswa
    ) AS mahasiswa_dengan_jalur
WHERE
    jalur_masuk IS NOT NULL
GROUP BY
    jalur_masuk
ORDER BY
    rata_rata_ipk DESC;
    

-- Insight 12: Kelulusan Mahasiswa di Setiap Mata Kuliah
-- Menghitung jumlah mahasiswa yang lulus (nilai selain D dan E) dan tidak lulus (nilai D atau E)
-- untuk setiap mata kuliah, guna mengidentifikasi mata kuliah yang paling menantang.
SELECT
    mk.kode_mk,
    mk.nama_mk,
    SUM(CASE
            WHEN n.huruf_nilai NOT IN ('D', 'E') THEN 1
            ELSE 0
        END) AS jumlah_lulus,
    SUM(CASE
            WHEN n.huruf_nilai IN ('D', 'E') THEN 1
            ELSE 0
        END) AS jumlah_tidak_lulus
FROM
    Fact_Transkrip ft
JOIN
    Dim_MataKuliah mk ON ft.id_mk = mk.id_mk
JOIN
    Dim_Nilai n ON ft.id_nilai = n.id_nilai
GROUP BY
    mk.id_mk, mk.kode_mk, mk.nama_mk
ORDER BY
    jumlah_tidak_lulus DESC, jumlah_lulus ASC;
