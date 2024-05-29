-- Menggunakan database world_layoff
use world_layoff;

-- Membuat salinan dari tabel layoffs ke tabel baru bernama layoffs_backup
create table layoffs_backup as
	select *
	from layoffs;

-- Menggunakan CTE untuk mengidentifikasi baris duplikat berdasarkan beberapa kolom kunci
with duplicate_cte as
(select
	*,
	row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as duplicate_row
from layoffs_backup)
select * from duplicate_cte
where duplicate_row > 1;

-- Membuat tabel baru untuk menyimpan data yang telah dibersihkan dari duplikasi
CREATE TABLE `layoffs_cleaned` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `duplicate_row` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Memverifikasi tabel baru
select * from layoffs_cleaned;

-- Memasukkan data dari layoffs_backup ke layoffs_cleaned dengan menambahkan kolom duplicate_row
insert into layoffs_cleaned
	select *,
        row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as duplicate_row
from layoffs_backup;

-- Menghapus semua baris duplikat, menyisakan hanya satu baris unik
delete 
from layoffs_cleaned
where duplicate_row > 1;

-- Memverifikasi bahwa tidak ada lagi baris yang memiliki duplicate_row lebih besar dari 1
select * from layoffs_cleaned
where duplicate_row > 1;

-- STANDARDISASI DATA
-- Memeriksa kolom company untuk spasi yang tidak perlu
select
	company,
	trim(company)
from layoffs_cleaned;

-- Menghapus spasi yang tidak perlu di kolom company
update layoffs_cleaned
set company = trim(company);

-- Menampilkan daftar unik dari kolom industry untuk melihat variasi data
select
	distinct industry
from layoffs_cleaned
order by 1 asc;

-- Menampilkan baris yang memiliki industry yang dimulai dengan 'Crypto'
select
	*
from layoffs_cleaned
where industry like 'Crypto%';

-- Menstandarisasi nilai di kolom industry yang berkaitan dengan 'Crypto'
update layoffs_cleaned
set industry = 'Crypto'
where industry like 'Crypto%';

-- Memverifikasi perubahan pada kolom industry
select 
	distinct industry
from layoffs_cleaned
order by 1;

-- Menampilkan daftar unik dari kolom location
select
	distinct location
from layoffs_cleaned
order by 1;

-- Menampilkan daftar unik dari kolom country
select
	distinct country
from layoffs_cleaned
order by 1;

-- Menampilkan baris dengan country yang mengandung 'United States'
select
	*
from layoffs_cleaned
where country like 'United States%'
order by 8 desc;

-- Memeriksa dan menghapus titik di akhir nilai pada kolom country
select
	distinct country,
    trim(trailing '.' from country)
from layoffs_cleaned
order by 1;

-- Menghapus titik di akhir nilai pada kolom country
update layoffs_cleaned
set country = trim(trailing '.' from country);

-- Memverifikasi perubahan pada kolom country
select
	distinct country
from layoffs_cleaned
order by 1;

-- Mengonversi kolom date dari string ke format DATE
select
	date,
    str_to_date(date, '%m/%d/%Y') as date
from layoffs_cleaned;

-- Memperbarui kolom date ke format DATE
update layoffs_cleaned
set date = str_to_date(date, '%m/%d/%Y');

-- Mengubah tipe data kolom date menjadi DATE
alter table layoffs_cleaned
modify column `date` DATE;

-- Menampilkan baris yang memiliki industry bernilai NULL atau kosong
select
	*
from layoffs_cleaned
where industry is null
	or industry = '';

-- Menampilkan baris dengan perusahaan tertentu
select
	*
from layoffs_cleaned
where company in ('Airbnb', "Bally's Interactive", 'Carvana', 'Juul');

-- Mengatur kolom industry menjadi NULL jika nilainya kosong
update layoffs_cleaned
set industry = null
where industry = '';

-- Menggabungkan data berdasarkan company dan location untuk mengisi kolom industry yang kosong
select * from layoffs_cleaned as t1
join layoffs_cleaned as t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

-- Memperbarui kolom industry yang kosong dengan nilai dari baris yang cocok
update layoffs_cleaned as t1
join layoffs_cleaned as t2
	on t1.company = t2.company
    and t1.location = t2.location
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

-- Menampilkan baris dengan perusahaan tertentu untuk memverifikasi pembaruan
select
	*
from layoffs_cleaned
where company in ('Airbnb', "Bally's Interactive", 'Carvana', 'Juul');

-- Menampilkan baris yang memiliki total_laid_off dan percentage_laid_off bernilai NULL
select
	*
from layoffs_cleaned
where total_laid_off is null
	and percentage_laid_off is null;

-- Menghitung jumlah total_laid_off dan percentage_laid_off yang bernilai NULL
select
	total_laid_off,
    percentage_laid_off,
    count(*) as total_null
from layoffs_cleaned
where total_laid_off is null
	and percentage_laid_off is null
group by 
	total_laid_off,
    percentage_laid_off;

-- Menghapus baris yang memiliki total_laid_off dan percentage_laid_off bernilai NULL
delete 
from layoffs_cleaned
	where total_laid_off is null
    and percentage_laid_off is null;

-- Menghapus kolom duplicate_row dari tabel layoffs_cleaned
alter table layoffs_cleaned
drop column duplicate_row;

-- Memverifikasi tabel akhir setelah semua pembaruan
select * from layoffs_cleaned;
