-- СОЗДАЕМ ТЕСТОВУЮ ТАБЛИЦУ В POSTGRES ДЛЯ МИГРАЦИИ

CREATE TABLE students (
  student_id INT NOT NULL,
  student_name VARCHAR(60) 
);

INSERT INTO students 
    (student_id, student_name) 
VALUES 
    (0, 'Mike'),
    (2, NULL),
    (2, 'Ivan'),
    (3, 'Dima Ivanov'),
    (4, NULL);
